
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

    def county_load_fuel_fraction(self, county_8760, fuels):
        """
        Calculate by industry, employment size class, and end use the hourly
        fraction of total load
        """

        if type(county_8760) == pd.core.frame.DataFrame:

            county_load_f = county_8760.copy(deep=True)

        else:

            county_load_f = pd.read_parquet(county_8760)

        # self.county_load = self.county_load.set_index(0, inplace=True)
        county_load_f['fraction'] = np.nan


        county_load_f.fraction.update(
            county_load_f.MW.divide(county_load_f.MW.sum(level=[0,1,2]))
            )

        county_load_f['fraction'] = \
            county_load_f.fraction.astype('float16')

        county_neeu = np.stack(
            [county_load_f.index_get_level_values(n).values for n in range(0,3)],
            axis=1
            )

        county_neeu = pd.DataFrame(county_neeu,
                                   columns=['naics', 'Emp_Size','End_use'])

        # These will not sum to 1 b/c not all "other" fuel types are included
        county_neeu = county_neeu.drop_duplicates()

        county_neeu.loc[:, 'MECS_region'] = self.mecs_fips_dict.xs(county)[0]

        fuel_dfs = pd.merge(county_neeu, self.mecs_fuel_intensity,
                            on=['MECS_Region', 'naics', 'Emp_Size','End_use'],
                            how='inner')

        fuels_df.drop('MECS_region', axis=1, inplace=True)

        # county_load_f['MW'].update(
        #     tech_opp_fuels.MW.multiply(tech_opp_fuels.fraction)
        #     )
        #
        # tech_opp_fuels = tech_opp_fuels.drop(['fraction'], axis=1).reset_index()

        # Make sure county has GHGRP facilities
        if county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():

            fuel_dfs = pd.concat(
                [fuels_df, self.ghgrp_fuel_intensity.set_index(
                    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
                    ).xs(county, level='COUNTY_FIPS')],
                axis=0, ignore_index=False, sort=True
                )

        fuel_dfs = pd.pivot_table(
            fuel_dfs, index=['naics','Emp_Size','End_use'],
            columns='MECS_FT_byp', values='MMBtu_fraction', aggfunc=np.mean,
            fill_value=0
            )

        for f in fuels:

            if f not in fuels_df.columns:

                fuels_df.loc[:, f] = 0

            else:
                continue

        # fuel_dfs = pd.concat(
        #     [fuel_dfs[fuel_dfs[col].isin(fuels)] for col in ['MECS_FT',
        #                                                     'MECS_FT_byp']],
        #     axis=0,
        #     )

        # These will not sum to 1 for all naics-emp size-end use combinations
        # b/c not all "other" fuel types are included (e.g., biomass)
        county_load_f = county_neeu.multiply(county_load_f.fraction, axis=0)

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

        return county_load_f

    def breakout_fuels_tech_opp(self, county, county_load_f, tech_opp):
        """
        Disaggregate tech opportunity by fuel type. Specify fuel as list.
        """

        tech_opp_fuels = tech_opp.join(county_load_f[['fraction']])

        # tech_opp_fuels['MW'].update(
        #     tech_opp_fuels.MW.multiply(tech_opp_fuels.fraction)
        #     )
        #
        # tech_opp_fuels = tech_opp_fuels.drop(['fraction'], axis=1).reset_index()
        #
        # MECS_region = self.mecs_fips_dict.xs(county)[0]
        #
        # # Make sure county has GHGRP facilities
        # if county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():
        #
        #     fuel_dfs = pd.concat(
        #         [self.mecs_fuel_intensity.set_index(
        #             ['MECS_Region', 'naics', 'Emp_Size','End_use']
        #             ).xs(MECS_region, level='MECS_Region'),
        #         self.ghgrp_fuel_intensity.set_index(
        #             ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
        #             ).xs(county, level='COUNTY_FIPS')],
        #         axis=0, ignore_index=False, sort=True
        #         )
        #
        # else:
        #
        #     fuel_dfs = self.mecs_fuel_intensity.set_index(
        #             ['MECS_Region', 'naics', 'Emp_Size','End_use']
        #             ).xs(MECS_region, level='MECS_Region')
        #
        # fuel_dfs = pd.concat(
        #     [fuel_dfs[fuel_dfs[col].isin(fuels)] for col in ['MECS_FT',
        #                                                     'MECS_FT_byp']],
        #     axis=0,
        #     )
        #
        # if fuel_dfs.empty:
        #
        #     tech_opp_fuels['MW'] = 0
        #
        # else:
        #
        #     # Drop duplicates.
        #     fuel_dfs = fuel_dfs.set_index('MECS_FT_byp', append=True)
        #
        #     fuel_dfs = fuel_dfs[~fuel_dfs.index.duplicated()]
        #
        #     fuel_dfs.reset_index('MECS_FT_byp', drop=False, inplace=True)
        #
        #     tech_opp_fuels = tech_opp_fuels.set_index(
        #         ['naics', 'Emp_Size', 'End_use']
        #         )
        #
        #     tech_opp_fuels = tech_opp_fuels.join(fuel_dfs,
        #                                          how='left').reset_index()
        #
        #     tech_opp_fuels['MW'].update(
        #         tech_opp_fuels.MW.multiply(tech_opp_fuels.MMBtu_fraction)
        #         )

        if len(fuels) <= 1:

            tech_opp_fuels = pd.DataFrame(
                tech_opp_fuels.groupby('index').MW.sum()
                )

            tech_opp_fuels = tech_opp_fuels.to_records(index=False)

        else:

            tech_opp_fuels = pd.DataFrame(
                tech_opp_fuels.groupby(['index', 'MECS_FT_byp']).MW.sum()
                )

            tech_opp_fuels.reset_index(['MECS_FT_byp'],drop=False,inplace=True)

            tech_opp_fuels.sort_index(ascending=True, inplace=True)

            tech_opp_fuels = pd.pivot(tech_opp_fuels, columns='MECS_FT_byp',
                                      values='MW')

            tech_opp_fuels = tech_opp_fuels.to_records(index=False)

        return tech_opp_fuels

    @staticmethod
    def breakout_county_ind(county_8760):
        """
        Creates an array of all 3-digit NAICS codes in county.
        """
        if type(county_8760) == pd.core.frame.DataFrame:

            county_ind = county_8760.copy(deep=True)

        else:

            county_ind = pd.read_parquet(county_8760)

        county_ind.reset_index(inplace=True)

        try:

            nlen = \
                all([len(str(x))==3 for x in county_ind['naics_sub'].unique()])

        except KeyError:

            naics_3d = np.array(
                [int(str(x)[0:3]) for x in county_ind.naics.unique()]
                )

        else:

            if nlen:

                naics_3d = county_ind['naics_sub'].unique()

        # county_ind = county_ind.groupby(
        #     ['index', naics_col]
        #     )[['MW', 'fraction']].sum()
        #
        # county_ind.fraction.update(
        #     county_ind.MW.divide(county_ind.MW.sum(level=0))
        #     )
        #
        # county_ind.drop(['MW'], axis=1, inplace=True)

        return naics_3d


    # @staticmethod
    # def breakout_ind_tech_opp(tech_opp, county_ind):
    #     """
    #     Creates a MultiIndex for the tech_opp dataframe, adding
    #     an index of 3-digit NAICS code to existing datetime index.
    #     Returns a structured array
    #     """
    #
    #     naics_index = county_ind.index.get_level_values(1)
    #
    #     naics_number = len(naics_index.unique())
    #
    #     tech_opp_ind = tech_opp.reindex(
    #         index=np.repeat(tech_opp.index, naics_number)
    #         )
    #
    #     tech_opp_ind[naics_index.name] = naics_index
    #
    #     tech_opp_ind.sort_index(ascending=True, inplace=True)
    #
    #     tech_opp_ind = pd.pivot(tech_opp_ind, columns=naics_index.name)
    #
    #     # tech_opp_ind.set_index([naics_index.name], append=True, inplace=True)
    #
    #     tech_opp_ind_array = tech_opp_ind.to_records(index=False)
    #
    #     return tech_opp_ind_array


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
