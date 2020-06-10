
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

    def process_results(self, results, county):
        """

        """

        if type(results) == pd.core.frame.DataFrame:

            self.county_load = results.copy(deep=True)

        else:

            self.county_load = pd.read_parquet(results)

        # Need to grab county FIPS
        self.county = county

        self.MECS_region = self.mecs_fips_dict.xs(county)[0]

        # self.county_load = self.county_load.set_index(0, inplace=True)

        # Calculate by industry, employment size class, and end use the hourly
        # fraction of total load
        self.county_load['fraction'] = np.nan

        self.county_load.fraction.update(
            self.county_load.MW.divide(self.county_load.MW.sum(level=[0,1,2]))
            )

        self.county_load['fraction'] = \
            self.county_load.fraction.astype('float32')


    def breakout_fuels_tech_opp(self, tech_opp, fuel):
        """
        Disaggregate tech opportnity by fuel type. Specify fuels as list.
        """

        tech_opp_fuels = tech_opp.join(self.county_load[['fraction']])

        tech_opp_fuels['MW'].update(
            tech_opp_fuels.MW.multiply(tech_opp_fuels.fraction)
            )

        tech_opp_fuels = tech_opp_fuels.drop(['fraction'], axis=1).reset_index()


        # Make sure county has GHGRP facilities
        if self.county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():

            fuel_dfs = pd.concat(
                [self.mecs_fuel_intensity.set_index(
                    ['MECS_Region', 'naics', 'Emp_Size','End_use']
                    ).xs(self.MECS_region, level='MECS_Region'),
                self.ghgrp_fuel_intensity.set_index(
                    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
                    ).xs(self.county, level='COUNTY_FIPS')],
                axis=0, ignore_index=False
                )

        else:

            fuel_dfs = self.mecs_fuel_intensity.set_index(
                    ['MECS_Region', 'naics', 'Emp_Size','End_use']
                    ).xs(self.MECS_region, level='MECS_Region')


        tech_opp_fuels = tech_opp_fuels.set_index(
            ['naics', 'Emp_Size', 'End_use']
            ).join(pd.concat(
                [fuel_dfs[
                    fuel_dfs[col].isin([fuel])
                    ] for col in ['MECS_FT', 'MECS_FT_byp']], axis=0,
                ).drop_duplicates(), how='left')

        tech_opp_fuels.reset_index(inplace=True)

        tech_opp_fuels['MW'].update(
            tech_opp_fuels.MW.multiply(tech_opp_fuels.MMBtu_fraction)
            )

        if len(fuel) > 1:

            tech_opp_fuels = tech_opp_fuels.groupby(
                ['MECS_FT_byp', 'index']
                ).MW.sum()

        else:

            tech_opp_fuels = tech_opp_fuels.groupby('index').MW.sum()

        return tech_opp_fuels


    def calc_tech_opp_net_fuel(self, MECS_FT):


        # Tech opp is abs(1 - 8760 gen/8760 demand)
        # Multiply the tech opp by self.county_load.fraction and then for each
        # fuel_type by the fuel mix of each naics-empsize-eu combination for
        # the county

        # multiply the result by self.county_load.fraction and then by
        # fuel mix by county (mecs_fuel_intesnsity.csv)
        # [need to map to MECS Region] to get fuels

        # Combine all results into hdf file?
        # datasets = 'tech_opp', tech_opp_ng, tech_opp_coal, tech_opp_rfo, land_use

        #
        return

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
