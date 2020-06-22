import pandas as pd
import numpy as np
import os
import sys
from format_rev_output import rev_postprocessing


class tech_opportunity(rev_postprocessing):

    def __init__(self, tech_package_name, rev_output_filepath, sizing_month=1):

        """
        Tech packages: 'swh', 'dsg_lf', 'ptc_notes', 'ptc_tes', 'pv_ac', or
        'pv_dc'
        """

        self.calc_datadir = './calculation_data/'

        self.tech_package_default = {
            'pv_hp': {'temp_range':[0,90],
                      'enduses':['Conventional Boiler Use'],
                      'industries': ['all'],
                      'rev_solar_tech': 'pv_ac'},
            'swh': {'temp_range':[0,90],
                    'enduses':['Conventional Boiler Use',
                               'CHP and/or Cogeneration Use'],
                    'industries':['all'],
                    'rev_solar_tech': 'pv_ac'},
            'pv_boiler': {'temp_range':[0,450],
                          'enduses':['Conventional Boiler Use',
                                     'CHP and/or Cogeneration Use'],
                          'industries': ['all'],
                          'rev_solar_tech': 'pv_ac'},
            'pv_resist': {'temp_range': [0, 800], 'enduses':['Process Heat'],
                          'industries': ['all'],
                          'rev_solar_tech': 'pv_ac'},
            'pv_whrhp':{'temp_range':[0,160],
                        'enduses':['Conventional Boiler Use',
                                   'CHP and/or Cogeneration Use',
                                   'Process Heat'],
                         'industries': ['all'],
                         'rev_solar_tech': 'pv_ac'},
            'dsg_lf': {'temp_range':[0,250],
                       'enduses':['Conventional Boiler Use',
                                  'CHP and/or Cogeneration Use','Process Heat'],
                         'industries': ['all'],
                         'rev_solar_tech': 'dsg_lf'},
            'ptc': {'temp_range': [0,400],
                    'enduses':['Conventional Boiler Use',
                               'CHP and/or Cogeneration Use','Process Heat'],
                     'industries': ['all'],
                     'rev_solar_tech':'ptc_tes'}
            }

        self.tech_package = self.tech_package_default[tech_package_name]

        # Instantiate methods for importing, formating, and
        # scaling reV output
        rev_postprocessing.__init__(
            self, rev_output_filepath,
            self.tech_package_default[tech_package_name]['rev_solar_tech']
            )

        # # import and format reV output for solar technology package
        self.rev_output = rev_postprocessing(
            rev_output_filepath,
            self.tech_package_default[tech_package_name]['rev_solar_tech']
            )

        # # import and format demand data
        # self.demand = demand_results(demand_filepath)
        #
        # # import methods and data for calculating load shape and peak
        # # load
        # self.demand_hourly = demand_hourly_load(2014, self.demand.demand_data)

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

    def get_county_info(self, county, county_ind):
        """
        Returns county FIPS, available land, and timezone
        """

        avail_land = self.rev_output.area_avail.xs(county)[
            'County Available area km2'
            ].astype('float32')

        timezone = self.rev_output.county_info.xs(county).timezone.astype('int')

        county_meta = pd.DataFrame(
            [[county, avail_land, timezone, county_ind]],
            columns=['COUNTY_FIPS', 'avail_land', 'timezone', 'industries']
            )

        county_meta = county_meta.to_records(index=False)

        return county_meta

    def tech_opp_county(self, county, county_8760_ophours, county_peak):
        """
        Calculates the technical opportunity for a single county. Returns
        a dictionary with values for the low/mean/high range of operating
        hours, as well as county information.
        """
        time_index = self.rev_output.gen_kW.index.values.astype(bytes)

        # Scale base-unit solar generation by peak demand
        # Returns the amount of land area used (in km2)
        scaled_generation, used_area_abs, used_area_pct = \
            self.rev_output.scale_generation(county, county_peak,
                                             month=self.sizing_month)

        county_8760_ophours.reset_index(
            ['naics', 'Emp_Size', 'End_use'], drop=True, inplace=True
            )

        county_8760_ophours = county_8760_ophours.groupby(
            county_8760_ophours.index
            ).MW.sum()

        tech_opp = pd.DataFrame(
            scaled_generation.MW.divide(county_8760_ophours)
            )
        ## Cap tech opp at 1?
        # tech_opp = pd.DataFrame(
        #     tech_opp.where(tech_opp < 1).fillna(1)
        #     ).sort_index(ascending=True)

        tech_opp_land = np.array([[used_area_abs]])

        return tech_opp, tech_opp_land
