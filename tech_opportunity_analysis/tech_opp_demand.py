
import pandas as pd
# import ghg
import os
import numpy as np


class demand_results:

    def __init__(self, demand_file):

        self.demand_data = pd.read_csv(demand_file, index_col=0)
        # Filter out non-CONUS counties
        self.demand_data = \
            self.demand_data[~self.demand_data.fipstate.isin([15, 2])]
        self.demand_data.drop(['MMBtu', 'fipstate'], axis=1, inplace=True)
        self.demand_data.rename(columns={'proc_MMBtu': 'MMBtu'}, inplace=True)

        # Map county FIPS from 2014 vintage to later vintage used by reV.
        self.demand_data.replace({'COUNTY_FIPS': {46113: 46102, 51515: 51019,
                                                  51595: 51081, 51690: 51089}},
                                 inplace=True)

        # Need to aggregate data due to introducing duplicate FIPS from
        # counties that were collapsed into other counties (e.g., 51595 >51081)
        self.demand_data = self.demand_data.groupby(
            ['COUNTY_FIPS', 'MECS_Region', 'naics', 'naics_sub', 'Emp_Size',
             'End_use', 'MECS_FT', 'Temp_C', 'data_source'], as_index=False
            )[['est_count', 'MMBtu']].sum()

        self.data_dir = './calculation_data'
        # Calculated using county_heat_calculations.ghg.py
        self.mecs_fuel_intensity = pd.read_csv(
            os.path.join(self.data_dir, 'mecs_fuel_intensity.csv')
            )

        # Calculated using county_heat_calculations.ghg.py
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
                self.ghgrp_fuel_intensity.sum(level=[0, 1, 2, 3])
                )
            ).reset_index()

        self.mecs_fips_dict = pd.read_csv(
            "../county_heat_calculations/calculation_data/US_FIPS_Codes.csv",
            usecols=['COUNTY_FIPS', 'MECS_Region'], index_col=['COUNTY_FIPS']
            )

    def county_load_fuel_fraction(self, county, county_8760, fuels):
        """
        Calculate by industry, employment size class, and end use the hourly
        fraction of total load.
        Specify fuels as list.
        """

        if type(county_8760) == pd.core.frame.DataFrame:
            county_load_f = county_8760.copy(deep=True)
        else:
            county_load_f = pd.read_parquet(county_8760)

        # self.county_load = self.county_load.set_index(0, inplace=True)
        county_load_f['fraction'] = np.nan

        # Calculate hourly fraction of total county load
        county_load_f.fraction.update(
            county_load_f.MW.divide(county_load_f.MW.sum(level=3))
            )
        county_load_f['fraction'] = \
            county_load_f.fraction.astype('float16')

        county_neeu = np.stack(
            [county_load_f.index.get_level_values(n).values for n in range(0,3)],
            axis=1
            )

        county_neeu = pd.DataFrame(county_neeu,
                                   columns=['naics', 'Emp_Size', 'End_use'])

        # These will not sum to 1 b/c not all "other" fuel types are included
        county_neeu = county_neeu.drop_duplicates()
        county_neeu.loc[:, 'MECS_Region'] = self.mecs_fips_dict.xs(county)[0]

        all_fuels = self.mecs_fuel_intensity.MECS_FT_byp.unique()

        if fuels == 'all':
            # There are counties that do not have any small establishments,
            # only GHGRP-reporting facilities
            fuel_dfs = pd.merge(county_neeu, self.mecs_fuel_intensity,
                                on=['MECS_Region', 'naics', 'Emp_Size',
                                    'End_use'], how='left')
            # Make sure county has GHGRP facilities before concatenating fuel
            # mix
            if county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():
                fuel_dfs = pd.concat(
                    [fuel_dfs, self.ghgrp_fuel_intensity.set_index(
                        ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
                        ).xs(county, level='COUNTY_FIPS').reset_index()],
                    axis=0, ignore_index=False, sort=True
                    )
        else:
            fuel_dfs = pd.merge(
                county_neeu, self.mecs_fuel_intensity[
                    self.mecs_fuel_intensity.MECS_FT_byp.isin(fuels)
                    ], on=['MECS_Region', 'naics', 'Emp_Size', 'End_use'],
                how='left'
                )
            # Make sure county has GHGRP facilities before concatenating fuel
            # mix
            if county in self.ghgrp_fuel_intensity.COUNTY_FIPS.unique():
                fuel_dfs = pd.concat(
                    [fuel_dfs, self.ghgrp_fuel_intensity[
                        self.ghgrp_fuel_intensity.MECS_FT_byp.isin(fuels)
                    ].set_index(
                        ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use']
                        ).xs(county, level='COUNTY_FIPS').reset_index()],
                    axis=0, ignore_index=False, sort=True
                    )

        fuel_dfs.drop(['MECS_FT', 'MECS_Region'], axis=1, inplace=True)
        # Drop any na values for instances where there are no small facilitiy
        # data from self.mecs_fuel_intensity
        fuel_dfs.dropna(inplace=True)
        fuel_dfs.set_index(['naics', 'Emp_Size', 'End_use'], inplace=True)
        #  Now getting IndexError: cannot do a non-empty take from an empty
        # axis
        try:
            fuel_dfs = pd.pivot_table(
                fuel_dfs, index=['naics', 'Emp_Size', 'End_use'],
                columns='MECS_FT_byp', values='MMBtu_fraction', aggfunc=np.mean
                )

        except IndexError as e:
            print("EXCEPTION:{}, in {}".format(e, county))
            fuel_dfs.reset_index().to_csv('fuel_dfs_{}.csv'.format(county))
            county_load_f.reset_index().to_csv(
                'county_load_f_{}.csv'.format(county)
                )
        else:
            # Ensure final dataframe contains all fuels
            fuel_dfs = pd.DataFrame(fuel_dfs, columns=all_fuels)
            fuel_dfs.fillna(0, inplace=True)

            # These will not sum to 1 for all naics-emp size-end use combinations
            # b/c not all "other" fuel types are included (e.g. some biomass types)
            county_load_f = fuel_dfs.multiply(county_load_f.fraction, axis=0)
        # Sum back to hourly, total county load, now split out into fuel types,
        county_load_f = county_load_f.sum(level=3)

        return county_load_f

    def calc_fuel_displaced(self, county_load_f, tech_opp, county_8760):
        """
        Disaggregate tech opportunity by fuel type. Output is the monthly
        sum of fuel displaced by solar technology in MWh.
        """

        # Tech_opp is the ratio of solar gen:demand, so value can be >1.
        # Constrain tech opportunity to 1 for fuel disaggregation
        fuel_displaced = county_load_f.multiply(
            tech_opp.where(tech_opp.MW < 1, 1).MW, axis=0
            )
        # Make sure index is sorted correctly as ascending datetime
        fuel_displaced = fuel_displaced.sort_index(ascending=True)

        # Multiply by total county hourly load (MW)
        fuel_displaced = fuel_displaced.multiply(county_8760.MW.sum(level=3),
                                                 axis=0)

        # Sum by month (MWh)
        fuel_displaced = fuel_displaced.resample('M').sum()
        # Send results to numpy record array
        fuel_displaced = fuel_displaced.to_records(index=False)

        return fuel_displaced

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
            nlen = all(
                [len(str(x)) == 3 for x in county_ind['naics_sub'].unique()]
                )
        except KeyError:
            naics_3d = np.array(
                [int(str(x)[0:3]) for x in county_ind.naics.unique()]
                )
        else:
            if nlen:
                naics_3d = county_ind['naics_sub'].unique()

        # Keep only unique values
        naics_3d = np.unique(naics_3d)

        return naics_3d
