import pandas as pd
import numpy as np
import os
import sys
from format_rev_output import rev_postprocessing
from tech_opp_demand import demand_results

sys.path.append('../')
from heat_load_calculations.run_demand_8760 import demand_hourly_load


class tech_opportunity:

    def __init__(self, tech_package_name, demand_filepath, rev_output_filepath,
                 sizing_month=1):

        """
        Tech packages: 'swh', 'dsg_lf', 'ptc_notes', 'ptc_tes', 'pv_ac', or
        'pv_dc'
        """

        # self.county_data = pd.read_parquet()

        self.calc_datadir = './calculation_data/'

        self.tech_package_default = {
            'pv_hp': {'temp_range':[0,90],
                      'enduses':['Conventional Boiler Use'],
                      'industries': ['all']},
            'swh': {'temp_range':[0,90],
                    'enduses':['Conventional Boiler Use',
                               'CHP and/or Cogeneration Use'],
                    'industries':['all']},
            'pv_boiler': {'temp_range':[0,450],
                          'enduses':['Conventional Boiler Use',
                                     'CHP and/or Cogeneration Use'],
                          'industries': ['all']},
            'pv_resist': {'temp_range': [0, 800], 'enduses':['Process Heat'],
                          'industries': ['all']},
            'pv_whrhp':{'temp_range':[0,160],
                        'enduses':['Conventional Boiler Use',
                                   'CHP and/or Cogeneration Use',
                                   'Process Heat'],
                         'industries': ['all']},
            'dsg_lf': {'temp_range':[0,250],
                       'enduses':['Conventional Boiler Use',
                                  'CHP and/or Cogeneration Use','Process Heat'],
                         'industries': ['all']},
            'ptc': {'temp_range': [0,400],
                    'enduses':['Conventional Boiler Use',
                               'CHP and/or Cogeneration Use','Process Heat'],
                     'industries': ['all']}
            }


        self.tech_package = self.tech_package_default[tech_package_name]

        # import and format reV output for solar technology package
        self.rev_output = rev_postprocessing(rev_output_filepath,
                                             tech_package_name)

        # import and format demand data
        self.demand = demand_results(demand_filepath)

        # import methods and data for calculating load shape and peak
        # load
        self.demand_hourly = demand_hourly_load(2014, self.demand.demand_data)

        # Set month to size generation (default=1 [January]). Default approach
        # is to size by month energy, not month peak power.
        self.sizing_month = 1

        # Fuel types to break out tech opportunity
        self.fuels_breakout = ['Natural_gas', 'Coal']

        # Specify operating hour ranges.
        self.op_hours = ['ophours_low', 'ophours_mean', 'ophours_high']

    def set_package_info(name, temp_range, enduses, industries):
        """
        Define package info.
        """

        new_info = {name: {'temp_range':temp_range, 'enduses':enduses,
                           'industries':industries}}

        self.tech_package[name] = new_info[name]

        print('Set', self.tech_package[name])


    def filter_county_data(self, mfg_heat_data):
        """
        Select county data based on a single specified technology package.
        Result is then passed to unit process calcuations.
        """

        enduses = self.tech_pacakge[tech_pacakge]['enduses']

        temp_range = self.tech_pacakge[tech_pacakge]['temp_range']

        industries = self.tech_pacakge[tech_pacakge]['industries']

        if industries == ['all']:

            selection = mfg_heat_data[
                (mfg_heat_data.End_use.isin(enduses)) &
                (mfg_heat_data.Temp_c.between(temp_range))
                ]
        else:

            selection = mfg_heat_data[
                (mfg_heat_data.End_use.isin(enduses)) &
                (mfg_heat_data.Temp_c.between(temp_range)) &
                (mfg_heat_data.naics.isin(industries))
                ]

        selection = selection.groupby(
            ['MECS_Region', 'COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use'],
            as_index=False
            ).MMBtu.sum()

        return selection

    def get_county_info(self, county):
        """
        Returns county FIPS, available land, and timezone
        """

        avail_land = self.rev_output.area_avail.xs(county)[
            'County Available area km2'
            ].astype('float16')

        timezone = self.rev_output.county_info.xs(county).timezone.astype('int')

        county_meta = np.array([[county], [avail_land], [timezone]])

        return county_meta


    def tech_opp_county(self, county):
        """
        Calculates the technical opportunity for a single county. Returns
        a dictionary with values for the low/mean/high range of operating
        hours, as well as county information.
        """

        # Calculate hourly load for annual demand (in MW). Calculate January
        #demand (in MWh) for sizing generation
        county_8760, county_peak = \
            self.demand_hourly.calculate_county_8760_and_peak(
                county, peak_month=self.sizing_month, peak_MW=False
                )

        time_index = self.rev_output.gen_kW.index.values

        first = True

        # Loop through mean, low, and high weekly operating hours
        for op_h in self.op_hours:

            county_8760_ophours = county_8760.xs(op_h, level='op_hours')

            # Assign breakouts by fuel and other characteristics
            county_fuel_fraction = self.demand.county_load_fuel_fraction(
                county_8760_ophours, county
                )

            peak_demand = county_peak.xs(op_h)[0]

            # Scale base-unit solar generation by peak demand
            # Returns the amount of land area used (in km2)
            scaled_generation, used_area_abs, used_area_pct = \
                self.rev_output.scale_generation(county, peak_demand,
                                                 month=self.sizing_month)

            county_8760_ophours.reset_index(
                ['naics', 'Emp_Size', 'End_use'], drop=True, inplace=True
                )

            county_8760_ophours = county_8760_ophours.groupby(
                county_8760_ophours.index
                ).MW.sum()

            tech_opp = scaled_generation.MW.divide(county_8760_ophours)

            tech_opp = pd.DataFrame(
                tech_opp.where(tech_opp < 1).fillna(1)
                ).sort_index(ascending=True)

            # tech_opp_array = tech_opp.values
            #
            # tech_opp_array.shape = (len(tech_opp), 1)

            if first:

                names = np.array([[op_h+'_techopp']])

                tech_opp_all = tech_opp.values

                tech_opp_land = np.array([[used_area_abs]])

            else:

                names = np.append(names, np.array([[op_h+'_techopp']]),
                                  axis=1)

                tech_opp_all = np.hstack([tech_opp_all, tech_opp.values])

                tech_opp_land = np.vstack([tech_opp_land,
                                           np.array([[used_area_abs]])])

            for fuel in self.fuels_breakout:

                names = np.append(names, np.array([[op_h+'_techopp'+'_'+fuel]]),
                                  axis=1)

                # All fuels in fuels_breakout may not be used in the county
                # Returns a dataframe of zeros if that is the case
                tech_opp_fuel = self.demand.breakout_fuels_tech_opp(
                    county, county_fuel_fraction, tech_opp, [fuel]
                    )

                tech_opp_fuel = \
                    tech_opp_fuel.sort_index().values

                tech_opp_fuel.shape = (len(tech_opp_fuel), 1)

                tech_opp_all = np.hstack([tech_opp_all, tech_opp_fuel])

            first = False

        tech_opp_meta = self.get_county_info(county)

        return names, time_index, tech_opp_all, tech_opp_meta, tech_opp_land


        #
        # to_array_columns = [op_h+'_techopp' for op_h in op_hours]
        #
        # to_array_columns = list(set(to_array_columns).union(
        #     [techopp+'_'+f for techopp in to_array_columns for f in fuels_breakout]
        #     ))
        #
        # to_array_columns.insert(0, 'time_index')

            # Final calc should return 3 arrays:
        # tech_opp.shape == (8761, len(to_array_columns)+1)
        # tech_opp[0,:] = ['timeindex','ophours_low_techopp'...'ophours_low_techopp_f1']
        # tech_opp_meta.shape == (county,)

        # tech_opp_land.shape == (3,1) [[o]]
        #
