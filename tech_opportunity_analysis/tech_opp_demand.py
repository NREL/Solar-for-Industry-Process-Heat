
import pandas as pd
import ghg
import os
import numpy as np

class demand_results:

    def __init__(self, demand_file):

        self.demand_data = pd.read_csv(demand_file, index_col=0)

        # Filter out non-CONUS counties
        self.demand_data = \
            self.demand_data[~self.demand_data.fipstate.isin([15,2])]

        self.demand_data.drop(['MMBtu', 'fipstate'], axis=1, inplace=True)

        self.demand_data.rename(columns={'proc_MMBtu': 'MMBtu'}, inplace=True)

        self.data_dir = './calculation_data'

        # Calculated using heat_load_calculations.ghg.py
        self.mecs_fuel_intensity = pd.read_csv(
            os.path.join(self.data_dir, 'mecs_fuel_intensity.csv')
            )

        # Calculated using heat_load_calculations.ghg.py
        self.ghgrp_fuel_intensity = pd.read_csv(
            os.path.join(self.data_dir, 'ghgrp_fuel_disagg_2014.csv')
            )

        self.ghgrp_fuel_intensity['Emp_Size'] = 'ghgrp'

        self.ghgrp_fuel_intensity = self.ghgrp_fuel_intensity.groupby(
            ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT',
             'MECS_FT_byp']
            ).MMBtu_fraction.sum()

        self.ghgrp_fuel_intensity = pd.DataFrame(
            self.ghgrp_fuel_intensity.divide(
                self.ghgrp_fuel_intensity.sum(level=[0,1,2,3])
                )
            ).reset_index()

        self.mecs_fips_dict = pd.read_csv(
            "../county_heat_calculations/calculation_data/US_FIPS_Codes.csv",
            usecols=['COUNTY_FIPS', 'MECS_Region'], index_col=['COUNTY_FIPS']
            )

    def county_load_fuel_fraction(self, county_8760, county):
        """

        """

        if type(county_8760) == pd.core.frame.DataFrame:

            county_load_ff = county_8760.copy(deep=True)

        else:

            county_load_ff = pd.read_parquet(county_8760)

        # self.county_load = self.county_load.set_index(0, inplace=True)

        # Calculate by industry, employment size class, and end use the hourly
        # fraction of total load
        county_load_ff['fraction'] = np.nan

        county_load_ff.fraction.update(
            county_load_ff.MW.divide(county_load_ff.MW.sum(level=[0,1,2]))
            )

        county_load_ff['fraction'] = \
            county_load_ff.fraction.astype('float16')

        return county_load_ff


    def breakout_fuels_tech_opp(self, county, county_load_ff, tech_opp, fuels):
        """
        Disaggregate tech opportnity by fuel type. Specify fuel as list.
        """

        tech_opp_fuels = tech_opp.join(county_load_ff[['fraction']])

        tech_opp_fuels['MW'].update(
            tech_opp_fuels.MW.multiply(tech_opp_fuels.fraction)
            )

        tech_opp_fuels = tech_opp_fuels.drop(['fraction'], axis=1).reset_index()

        MECS_region = self.mecs_fips_dict.xs(county)[0]

        # Make sure county has GHGRP facilities
        if county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():

            fuel_dfs = pd.concat(
                [self.mecs_fuel_intensity.set_index(
                    ['MECS_Region', 'naics', 'Emp_Size','End_use']
                    ).xs(MECS_region, level='MECS_Region'),
                self.ghgrp_fuel_intensity.set_index(
                    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
                    ).xs(county, level='COUNTY_FIPS')],
                axis=0, ignore_index=False, sort=True
                )

        else:

            fuel_dfs = self.mecs_fuel_intensity.set_index(
                    ['MECS_Region', 'naics', 'Emp_Size','End_use']
                    ).xs(MECS_region, level='MECS_Region')

        fuel_dfs = pd.concat(
            [fuel_dfs[fuel_dfs[col].isin(fuels)] for col in ['MECS_FT',
                                                            'MECS_FT_byp']],
            axis=0,
            )

        if fuel_dfs.empty:

            tech_opp_fuels['MW'] = 0

        else:

            # Drop duplicates.
            fuel_dfs = fuel_dfs.set_index('MECS_FT_byp', append=True)

            fuel_dfs = fuel_dfs[~fuel_dfs.index.duplicated()]

            fuel_dfs.reset_index('MECS_FT_byp', drop=False, inplace=True)

            tech_opp_fuels = tech_opp_fuels.set_index(
                ['naics', 'Emp_Size', 'End_use']
                )

            tech_opp_fuels = tech_opp_fuels.join(fuel_dfs,
                                                 how='left').reset_index()

            tech_opp_fuels['MW'].update(
                tech_opp_fuels.MW.multiply(tech_opp_fuels.MMBtu_fraction)
                )

        tech_opp_fuels = tech_opp_fuels.groupby('index').MW.sum()

        return tech_opp_fuels


    # This should be a separate file and class.
    # Need to pull out tech opp 8760, then calculate savings
    def calc_net_ghgs(self, tech_opp, county):

        # Sum fuel use to annual and by fuel and Emp_Size.
        # Separate calculations for ghgrp and non-ghgrp
        ##
        opp_ghgs = tech_opp.multiply(
            tech_self.load_fraction.reset_index().groupby(
                ['index','naics', 'Emp_Size']
                ).fraction.sum()
            )

        opp_ghgs = pd.DataFrame(opp_ghgs.sum(level=[1,2]))

        # Also NEED MECS REGION
        opp_ghgs['COUNTY_FIPS'] = self.county

        opp_

        return
