import pandas as pd
import numpy as np
import os
import sys
from format_rev_output import rev_postprocessing
from tech_opp_demand import demand_results

sys.path.append('../')
from heat_load_calculations.run_demand_8760 import demand_hourly_load


class tech_potential:

    def __init__(self, tech_package_name, demand_filepath, rev_output_filepath):

        """
        Tech packages: 'swh', 'pv_hp', 'pv_boiler', 'pv_resist'
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


    def set_package_info(name, temp_range, enduses, industries):
        """
        Define
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

    def calc_tech_opp(self, county):
        """

        """
    # Make a method to calculate tech opp for a county here.
    # Add process_county_results and calc_tech_opp from tech_opp_demand.py

        # Month used to calculate peak demand and to scale generation
        month=1

        # Set fuels to break out tech opportunity
        fuels_breakout = ['Natural_gas', 'Coal']

        op_hours = ['ophours_mean', 'ophours_low','ophours_high']

        # Calculate hourly load for annual demand (in MW). Calculate January
        #demand (in MWh) for sizing generation
        county_8760, county_peak = \
            self.demand_hourly.calculate_county_8760_and_peak(county,
                                                              peak_month=month,
                                                              peak_MW=False)

        def create_empty_dict(list):

            tech_opp_dict = dict(
                [(i, np.empty([8760,1])) for i in list]
                )

            return tech_opp_dict

        tech_opp_all = create_empty_dict(op_hours)

        # Loop through mean, low, and high weekly operating hours
        for op_h in op_hours:

            print(op_h)

            tech_opp_fuels = create_empty_dict(fuels_breakout)

            peak_demand = county_peak.xs(op_h)[0]

            # Scale base-unit solar generation by peak demand
            # Also returns the amount of land area used (in km2) and the
            # fraction of available land area.
            scaled_generation, used_area_abs, use_area_pct = \
                self.rev_output.scale_generation(county, peak_demand,
                                                 month=month)

            county_8760_ophours = county_8760.xs(op_h, level='op_hours')


            print(scaled_generation.head())

            # Assign breakouts by fuel and other characteristics
            self.demand.process_results(county_8760_ophours, county)

            tech_opp = county_8760_ophours.reset_index(
                ['naics', 'Emp_Size', 'End_use'], drop=True
                )

            tech_opp = tech_opp.groupby(tech_opp.index).MW.sum()

            print(tech_opp.head())

            tech_opp = abs(1 - tech_opp.divide(scaled_generation.MW))

            tech_opp = pd.DataFrame(tech_opp.where(tech_opp < 1).fillna(1))

            for ft in fuels_breakout:

                tech_opp_fuels[ft] = \
                    self.demand.breakout_fuels_tech_opp(tech_opp, ft)

            tech_opp_all[op_h] = tech_opp

            tech_op_all[op_h+'_by_fuel'] = tech_opp_fuels

            tech_opp_all[op_h+'_land'] = dict(
                [('abs', used_area_abs), ('pct_of_avail', used_area_pct)]
                )

        return tech_opp_all
