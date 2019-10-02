import pandas as pd
import numpy as np
import itertools as itools
import os
import re
import dask.dataframe as dd
import enduse_temps_IPH


class Manufacturing_energy:
    """
    Calcualtes energy for a single year, combining energy values for GHGRP
    facilities calculated by GHGRP_config_IPH.py, with energy values calculated
    below for remaining manufacturing facilities.

    """

    #Set analysis year and required file paths
    def __init__(self, year, energy_ghgrp):

        self.year = year

        if self.year > 2012:

            self.naics_column = 'PRIMARY_NAICS_CODE_12'

        else:

            self.naics_column = 'PRIMARY_NAICS_CODE'

        self.file_dir = '../calculation_data/'

        self.fuelxwalk_file = 'MECS_FT_IPF.csv'

        self.naics_2012_file = 'mecs_naics_2012.csv'

        self.naics_old_file = 'mecs_naics.csv'

        self.ipf_results_file = 'mecs_'+str(self.year)+\
            '_ipf_results_naics_employment.csv'

        self.mecs_naics = pd.DataFrame()

        self.end_use_file = 'table5_2_' + str(self.year) + '_formatted.csv'

        for file in [self.naics_2012_file, self.naics_old_file]:

            mdf = pd.read_csv(os.path.join(self.file_dir, file))

            if file == 'mecs_naics.csv':

                mdf['vintage'] = 2010

            else:

                mdf['vintage'] = 2012

            self.mecs_naics = self.mecs_naics.append(mdf, ignore_index=True)

        self.fuelxwalkDict = dict(pd.read_csv(
                os.path.join(self.file_dir, self.fuelxwalk_file)
                )[["EPA_FUEL_TYPE", "MECS_FT"]].values)

        self.empsize_dict = {'Under 50': 'n1_49', '50-99': 'n50_99',
                             '100-249': 'n100_249','250-499': 'n250_499',
                             '500-999': 'n500_999','1000 and Over': 'n1000'}

        self.energy_ghgrp_y = pd.DataFrame(
                energy_ghgrp[energy_ghgrp.REPORTING_YEAR == self.year]
                )

    def update_naics(self, ghgrp_matching):
        """
        Import list of NAICS codes used in MECS. Need to account for CBP data
        after 2011 use 2012 NAICS, while MECS and GHGRP use 2007 NAICS.
        """

        def MatchMECS_NAICS(DF, naics_column):
            """
            Method for matching 6-digit NAICS codes with adjusted
            MECS NAICS.
            """
            DF[naics_column].fillna(0, inplace = True)

            DF.loc[:, naics_column] = DF[naics_column].astype('int')

            DF_index = DF[DF[naics_column].between(310000, 400000,
                          inclusive=False)]

            #split ghgrp data into pre- and post-2012 to account for the
            # change in NAICS base year for CBP data.

            nctest = DF.loc[DF_index.index, [naics_column, 'REPORTING_YEAR']]

            for n in ['N6', 'N5', 'N4', 'N3']:

                n_level = int(n[1])

                nctest[n] = nctest[naics_column].apply(
                        lambda x: int(str(x)[0:n_level]))


#            nctest = \
#                [DF.loc[DF_index.index,
#                        [naics_column, 'REPORTING_YEAR']].dropna().apply(
#                            lambda x: int(str(x)[
#                                    0:len(str(x))- i
#                                    ])
#                        ) for i in range(0,4)]
#
#            nctest = pd.concat(nctest, axis = 1)
#
#            nctest.columns = ['N6', 'N5', 'N4', 'N3']

            #Match GHGRP NAICS to highest-level MECS NAICS. Will match to
            #"dummy" "-09" NAICS where available. This is messy, but
            #functional.
            if self.year < 2012:

                ncmatch = pd.concat(
                    [pd.merge(nctest,
                              self.mecs_naics[self.mecs_naics.vintage == 2010],
                              left_on=column, right_on='MECS_NAICS',
                              how= 'left')['MECS_NAICS']
                        for column in ['N6', 'N5', 'N4', 'N3']], axis=1
                    )

            else:

                ncmatch = pd.concat(
                    [pd.merge(nctest,
                              self.mecs_naics[self.mecs_naics.vintage == 2012],
                              left_on=column, right_on='MECS_NAICS',
                              how='left')['MECS_NAICS']
                        for column in ['N6', 'N5', 'N4', 'N3']], axis =1
                    )

            ncmatch.columns = ['N6', 'N5', 'N4', 'N3']

            ncmatch.index = nctest.index

            ncmatch['NAICS_MATCH'] = 0

            for n in range(3, 7):

                column = 'N'+str(n)

                ncmatch.NAICS_MATCH.update(ncmatch[column].dropna())

#            for dataframe in ncmatch.keys():
#
#                ncmatch[dataframe]['NAICS_MATCH'] = ncmatch[dataframe].apply(
#                        lambda x: int(list(x.dropna())[0]), axis=1
#                        )
#
#                DF['MECS_NAICS'].update(ncmatch[dataframe].NAICS_MATCH)
#
#                ncmatch_y = pd.concat(
#                    [pd.merge(nctest[dataframe], MECS_NAICS,
#                              left_on=nctest[dataframe][column],
#                              right_on = MECS_NAICS.MECS_NAICS,
#                              how = 'left').iloc[:,4]
#                        for column in nctest[dataframe].columns], axis =1
#                    )
#
#                ncmatch_y.index = nctest[dataframe].index
#
#                ncmatch_y['NAICS_MATCH'] = ncmatch_y.apply(
#                    lambda x: int(list(x.dropna())[0]), axis = 1
#                    )
#
#                ncmatch = ncmatch.append(ncmatch_y, ignore_index=False)

            #Update GHGRP dataframe with matched MECS NAICS.
            DF['MECS_NAICS'] = 0

            DF['MECS_NAICS'].update(ncmatch.NAICS_MATCH)

            return DF


        # Map EPA fuel types to MECS fuel types. Note this doens't cover all
        # custom fuel types in GHGRP.
        self.energy_ghgrp_y['MECS_FT'] = np.nan

        for f in ['FUEL_TYPE_OTHER','FUEL_TYPE_BLEND', 'FUEL_TYPE']:

            self.energy_ghgrp_y['MECS_FT'].update(
                    self.energy_ghgrp_y[f].map(self.fuelxwalkDict)
                    )

        #Match GHGRP-reported 6-digit NAICS code with MECS NAICS

        self.energy_ghgrp_y = \
            pd.merge(self.energy_ghgrp_y,
                     ghgrp_matching[['FACILITY_ID',
                                     self.naics_column]],
                     on='FACILITY_ID', how='left')

        self.energy_ghgrp_y = MatchMECS_NAICS(
                self.energy_ghgrp_y, 'PRIMARY_NAICS_CODE'
                )

        # Filter out facilities that use PRIMARY_NAICS_CODE == 486210 and
        # NAICS_USED == 0
        self.energy_ghgrp_y = self.energy_ghgrp_y[
                (self.energy_ghgrp_y[self.naics_column] != 486210)
                ].copy(deep=True)


#        return self.energy_ghgrp_y


    def GHGRP_Totals_byMECS(self):
        """
        From calculated GHGRP energy data, create sums by MECS Region,
        MECS NAICS and MECS fuel type for a given MECS year.
        """

        ghgrp_mecs = pd.DataFrame(
            self.energy_ghgrp_y[self.energy_ghgrp_y.MECS_NAICS != 0][
                        ['MECS_Region', 'MECS_NAICS', 'MECS_FT','MMBtu_TOTAL']
                        ]
            )

        ghgrp_mecs.dropna(inplace = True)

        ghgrp_mecs['MECS_R_FT'] = ghgrp_mecs['MECS_Region'] + '_' + \
            ghgrp_mecs['MECS_FT']

        r_f = []

        for r in ['Midwest', 'Northeast', 'South', 'West']:

            r_f.append([r + '_' + c + '_Total' for c in ghgrp_mecs[
                    ghgrp_mecs.MECS_Region == r
                    ].MECS_FT.dropna().unique()])

        for n in range(len(r_f)):
            r_f[n].append(r_f[n][1].split("_")[0] + "_Total_Total")

        if self.year < 2012:

            ghgrp_mecstotals = pd.DataFrame(
                index=self.mecs_naics[
                    self.mecs_naics.vintage == 2010
                    ].MECS_NAICS_dummies, columns=np.array(r_f).flatten()
                )

        else:

            ghgrp_mecstotals = pd.DataFrame(
                index=self.mecs_naics[
                    self.mecs_naics.vintage == 2012
                    ].MECS_NAICS_dummies, columns=np.array(r_f).flatten()
                )

        for name, group in ghgrp_mecs.groupby(['MECS_R_FT', 'MECS_NAICS'])[
            'MMBtu_TOTAL']:
                ghgrp_mecstotals.loc[int(name[1]), name[0] + '_Total'] = \
                    group.sum()

        for name, group in ghgrp_mecs.groupby(['MECS_Region', 'MECS_NAICS'])[
            'MMBtu_TOTAL']:
                ghgrp_mecstotals.loc[
                    int(name[1]), name[0] + '_Total_Total'] = group.sum()

        ghgrp_mecstotals.fillna(0, inplace=True)

        # Convert from MMBtu to TBTu
        ghgrp_mecstotals = ghgrp_mecstotals/10**6

        return ghgrp_mecstotals


#    def GHGRP_electricity_calc(GHGRP_electricity, cbp_matching):
#        """
#        Requires running format_eia923() from EIA_CHP.py
#        """
#
#        EIA923_2014counts = pd.DataFrame(
#            GHGRP_electricity.groupby('FIPS_NAICS')['FACILITY_ID'].count()
#            )
#
#        EIA923_2014counts.rename(
#            columns={'FACILITY_ID':'fac_count923'}, inplace=True
#            )
#
#        EIA923_2014counts.loc[:, 'FIPS_NAICS'] = EIA923_2014counts.index.values
#
#        EIA923_2014counts = EIA923_2014counts.merge(
#            cbp_matching[['ghgrp_fac', 'fips_n']], left_index=True,
#                right_on = 'fips_n'
#            )
#
#        # Create new corrections of CBP facility counts where the number of
#        # EIA923 facilities != number of GHGRP facilities.
#        fips_naics_923 = EIA923_2014counts[(
#            EIA923_2014counts.fac_count923 != EIA923_2014counts.ghgrp_fac
#            )]
#
#        cbp_formatching_923 = pd.merge(cbp_matching, fips_naics_923[
#                ['fac_count923', 'fips_n']], on='fips_n'
#            )
#
#        # The following should be made into a method based on method in
#        # Match_GHGRP_County.py.
#        large = ['n50_99', 'n100_249', 'n250_499', 'n500_999', 'n1000']
#
#        small = ['n1_4', 'n5_9', 'n10_19', 'n20_49']
#
#        for i in cbp_formatching_923.index:
#            if cbp_formatching_923.loc[i,'fac_count923'] > cbp_formatching_923.loc[
#                i,'est']:
#
#                count = cbp_formatching_923.loc[i, 'est']
#
#            else:
#                count = cbp_formatching_923.loc[i, 'fac_count923']
#
#            while count > 0:
#                maxsize = [c for c in itools.compress(small + large,
#                    cbp_formatching_923.ix[i, ('n1_4'):('n1000')].values
#                    )][-1]
#
#                cbp_formatching_923.loc[i, maxsize] = cbp_formatching_923.loc[
#                    i, maxsize] - 1
#
#                count = count - 1
#
#            cbp_formatching_923.loc[i, 'est_large_corrected'] = \
#                cbp_formatching_923.loc[i, ('n50_99'):('n1000')].sum()
#
#            cbp_formatching_923.loc[i, 'est_small_corrected'] = \
#                cbp_formatching_923.loc[i, ('n1_4'):('n20_49')].sum()
#
#        cbp_formatching_923.loc[:, 'n1_49'] = cbp_formatching_923[[
#            'n1_4', 'n5_9', 'n10_19', 'n20_49'
#            ]].sum(axis=1)
#
#        # Reindex to match corresponding cbp_matching index values
#        cbp_formatching_923.loc[:, 'cbpfm_i']  = [
#            cbp_matching[cbp_matching.fips_n == n].index[0] for n in \
#                cbp_formatching_923.fips_n
#            ]
#
#        cbp_formatching_923.set_index(['cbpfm_i'], inplace=True)
#
#        cbp_corrected_923 = pd.DataFrame(cbp_matching, copy=True)
#
#        cbp_corrected_923.update(cbp_formatching_923)
#
#        return cbp_corrected_923


    def calc_intensities(self, cbp_matching):
        """
        Calculate MECS intensities (energy per establishment) based on 2010 or
        2014 CBP establishment counts and IPF results.
        Note that datasets don't match perfectly-- i.e., results of 'NaN'
        indicate that IPF calculated an energy value for a MECSs region, NAICS,
        and facility count that corresponds to a zero CBP facility count;
        results of 'inf' indicate a nonzero CBP facility count for a
        MECS region, NAICS, and facility count with an IPF-caculated energy
        value of zero.
        """

        #Format results from IPF of MECS energy data by region, fuel type,
        #and employment size.

        ipf_results_formatted = pd.read_csv(
                self.file_dir+self.ipf_results_file, index_col=0
                )

        mecs_intensities = pd.melt(
                ipf_results_formatted,
                id_vars=['MECS_Region', 'Emp_Size', 'MECS_FT'],
                var_name=['MECS_NAICS_dummies'], value_name='energy'
                )

        mecs_intensities['MECS_NAICS_dummies'] =\
            mecs_intensities.MECS_NAICS_dummies.astype('int')

        mecs_intensities.set_index(
                ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size'],
                inplace=True
                )

        cbp_grpd = cbp_matching.groupby(
                ['MECS_Region', 'MECS_NAICS_dummies'], as_index=False
                ).sum()

        cbp_grpd = pd.melt(
                cbp_grpd, id_vars=['MECS_Region', 'MECS_NAICS_dummies'],
                value_vars=[x for x in self.empsize_dict.values()],
                var_name=['Emp_Size'], value_name='est_count'
                )

        cbp_grpd.set_index(
                ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size'],
                inplace=True
                )

        mecs_intensities = mecs_intensities.join(cbp_grpd)

        mecs_intensities['intensity'] =\
            mecs_intensities.energy.divide(mecs_intensities.est_count,
                                           fill_value=0)

        mecs_intensities.drop(['energy', 'est_count'], axis=1, inplace=True)

        mecs_intensities.reset_index(inplace=True)

        #Fill NaN values for intensities with 0.
        mecs_intensities.fillna(0, inplace=True)

        mecs_intensities.replace(np.inf, 0, inplace=True)

        #Create tuples of fuel type and employment size for future matching
#        mecs_intensities["FT_Emp"] = [
#            z for z in zip(
#                mecs_intensities.MECS_FT.values, \
#                    mecs_intensities.Emp_Size.values
#                )
#            ]

        return mecs_intensities

    def combfuel_calc(self, cbp_corrected, mecs_intensities):

        """
        Calculate county-level manufacturing energy use based on CBP facility
        counts, calculated MECS intensities, and calculated facility energy use
        for GHGRP facilites.
        Net electricity undergoes an additional adjustment.

        Returns a Dask DataFrame
        """

        energy_nonghgrp = pd.melt(
                cbp_corrected, id_vars=['fips_matching', 'MECS_NAICS_dummies',
                                        'MECS_Region', 'fipstate', 'fipscty',
                                        'naics', 'COUNTY_FIPS', 'MECS_NAICS'],
                value_vars=[x for x in self.empsize_dict.values()],
                var_name=['Emp_Size'], value_name='est_count'
                )

        # Need to set mecs_intensities index to include MECS_FT? Then reindex
        # energy_nonghgrp?

        # Drop MECS_NAICS_dummies == np.nan. These are non-manufacturing
        # industrial naics codes (i.e., ag, mining, and construction)
        energy_nonghgrp.dropna(subset=['MECS_NAICS_dummies'], inplace=True)

        energy_nonghgrp = dd.merge(
            energy_nonghgrp.set_index(
                ['MECS_NAICS_dummies']
                ), mecs_intensities[
                    mecs_intensities.MECS_FT != 'Net_electricity'
                    ].set_index(
                        ['MECS_NAICS_dummies']
                        ), on=['MECS_NAICS_dummies','MECS_Region','Emp_Size'],
            how='inner'
            )


#        energy_nonghgrp = pd.merge(
#            energy_nonghgrp.set_index(
#                ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size']
#                ), mecs_intensities[
#                    mecs_intensities.MECS_FT != 'Net_electricity'
#                    ].set_index(
#                        ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size']
#                        ), left_index=True, right_index=True, how='inner'
#            )

        energy_nonghgrp.reset_index(inplace=True)

        energy_nonghgrp['MMBtu_TOTAL'] = energy_nonghgrp.est_count.multiply(
                energy_nonghgrp.intensity, fill_value=0
                )*10**6

        #energy_nonghgrp.drop(['fips_matching'], axis=1, inplace=True)

        energy_nonghgrp['COUNTY_FIPS'] = \
            energy_nonghgrp.COUNTY_FIPS.astype(int)

#        energy_nonghgrp = energy_nonghgrp.groupby(
#                ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS',
#                 'MECS_FT', 'Emp_Size'], as_index=False
#                )[['MMBtu_TOTAL', 'est_count']].sum()

        energy_nonghgrp = energy_nonghgrp.groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS',
                 'MECS_FT', 'fipstate', 'fipscty', 'Emp_Size'], as_index=False
                )[['MMBtu_TOTAL', 'est_count']].sum()

        energy_nonghgrp['data_source'] = 'mecs_ipf'

        energy_ghgrp_y = self.energy_ghgrp_y.groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                 'MECS_NAICS','MECS_FT'], as_index=False
                ).MMBtu_TOTAL.sum()

        #Drop non-manufacturing industries (MECS_NAICS == 0)
        energy_ghgrp_y = energy_ghgrp_y[energy_ghgrp_y !=0]

        energy_ghgrp_y['data_source'] = 'ghgrp'

        energy_ghgrp_y['Emp_Size'] = 'ghgrp'

        energy_ghgrp_y.rename(columns={'PRIMARY_NAICS_CODE':'naics'},
                                       inplace=True)

        energy_ghgrp_y['COUNTY_FIPS'] = energy_ghgrp_y.COUNTY_FIPS.astype(int)

        fips_dict = energy_nonghgrp[
                ['COUNTY_FIPS', 'fipstate', 'fipscty']
                ].drop_duplicates().set_index('COUNTY_FIPS').to_dict('index')

        def match_countyfips(county_fips, fips_dict, fips_cat):

            if county_fips in fips_dict.keys():

                return fips_dict[county_fips][fips_cat]

            else:

                fips_len = len(str(county_fips))

                missing_dict = {
                        'fipstate': int(str(county_fips)[0:fips_len-3]),
                        'fipscty': int(str(county_fips)[fips_len-3:])
                        }

                return missing_dict[fips_cat]


        energy_ghgrp_y['fipscty'] = energy_ghgrp_y.COUNTY_FIPS.apply(
                lambda x: match_countyfips(x, fips_dict, 'fipscty')
                )

        energy_ghgrp_y['fipstate'] = energy_ghgrp_y.COUNTY_FIPS.apply(
                lambda x: match_countyfips(x, fips_dict, 'fipstate')
                )

        county_combustion_energy_dd = dd.from_pandas(energy_nonghgrp.append(
                energy_ghgrp_y, ignore_index=True, sort=True
                ).set_index('MECS_NAICS'),
                npartitions=len(self.mecs_naics.MECS_NAICS.unique()
                ))

        county_combustion_energy_dd['naics'] =\
                county_combustion_energy_dd.naics.astype('int')

        county_combustion_energy_dd = county_combustion_energy_dd.compute()

        return county_combustion_energy_dd

#    def elec_calc(GHGRP_electricity, CountyEnergy_wGHGRP, cbp_corrected_923,
#                  MECS_intensities):
#        """
#        Calculate net electricity based on EIA 923 data. First use values
#        calculated prior to correcting for GHGRP facilities.
#        """
#
#        GHGRP_electricity.set_index(['FIPS_NAICS'], drop=True, inplace=True)
#
#        r_df = pd.DataFrame(
#            index=cbp_corrected_923.dropna(subset=['MECS_NAICS']).index, \
#                columns=list(cbp_corrected_923.MECS_Region.drop_duplicates()
#                )
#            )
#
#        for r in cbp_corrected_923.MECS_Region.drop_duplicates():
#
#            fuel_df = pd.DataFrame(
#                index = \
#                    cbp_corrected_923.dropna(subset=['MECS_NAICS']).index,\
#                columns=['Net_electricity']
#                )
#
#            for n in cbp_corrected_923.loc[
#                fuel_df.index, 'MECS_NAICS'].drop_duplicates():
#
#                cbpi = \
#                    cbp_corrected_923[(cbp_corrected_923.MECS_Region == r) & \
#                        (cbp_corrected_923.MECS_NAICS == n)].index
#
#                fuel_sum = pd.DataFrame(index=cbpi)
#
#                for s in MECS_intensities.Emp_Size.drop_duplicates()[0:6]:
#                    fuel_sum.loc[:,s] = MECS_intensities[
#                        (MECS_intensities.MECS_Region == r) & \
#                            (MECS_intensities.FT_Emp == ('Net_electricity',s))
#                        ][n].values[0] * cbp_corrected_923.loc[cbpi,s]
#
#                fuel_sum = pd.DataFrame(
#                    fuel_sum.sum(axis=1), columns=['Net_electricity']
#                    )
#
#                fuel_df.update(fuel_sum, overwrite=True)
#
#                # fuel_df = pd.concat(
#                #    [fuel_df, fuel_sum.sum(axis = 1)], axis = 1, join = 'outer'
#                #    )
#
#            r_df[r] = fuel_df.sum(axis=1)
#
#        r_df.loc[:, 'FIPS_NAICS'] = cbp_corrected_923.dropna(
#            subset = ['MECS_NAICS']).fips_n
#
#        r_df.set_index(['FIPS_NAICS'], drop=True, inplace=True)
#
#        #Add column for electricity reported on Form EIA-923
#        r_df['elec923'] = GHGRP_electricity.groupby(
#            GHGRP_electricity.index
#            ).Net_electricity.sum()
#
#        CountyEnergy_wGHGRP.loc[r_df.index, 'Net_electricity'] = r_df.sum(
#            axis=1
#            )
#
#        return CountyEnergy_wGHGRP



    def calc_enduse(self, eu_fraction_dict, county_energy_dd, temps=False):
        """
        Calculates energy by end use based on unit type reported in GHGRP
        data and MECS end use data.
        Returns Dask DataFrame
        """
        unitname_eu_dict = {
                'Process Heating': ['furnace', 'kiln', 'dryer', 'heater',
                                    'oven','calciner', 'stove', 'htr', 'furn',
                                    'cupola'],
                'Conventional Boiler Use': ['boiler'],
                'CHP and/or Cogeneration Process': ['turbine'],
                'Facility HVAC': ['building heat', 'space heater'],
                'Machine Drive': ['engine','compressor', 'pump', 'rice'],
                'Conventional Electricity Generation': ['generator'],
                'Other Nonprocess Use': ['hot water', 'crane', 'water heater',
                                     'comfort heater', 'RTO', 'TODF',
                                     'oxidizer', 'RCO']
                }

        unittype_eu_dict = {
                'Process Heating': ['F', 'PD', 'K', 'PRH', 'O', 'NGLH', 'CF',
                                 'HMH', 'C', 'HPPU', 'CatH', 'COB', 'FeFL',
                                 'Chemical Recovery Furnace', 'IFCE',
                                 'Pulp Mill Lime Kiln', 'Lime Kiln',
                                 'Chemical Recovery Combustion Unit',
                                 'Direct Reduction Furnace',
                                 'Sulfur Recovery Plant'],
                'Conventional Boiler Use': ['OB', 'S', 'PCWW', 'BFB', 'PCWD',
                                        'PCT', 'CFB', 'PCO', 'OFB', 'PFB'],
                'CHP and/or Cogeneration Process': ['CCCT', 'SCCT'],
                'Facility HVAC': ['CH'],
                'Other Nonprocess Use': ['HWH', 'TODF', 'ICI', 'FLR', 'RTO',
                                         'II', 'MWC', 'Flare', 'RCO' ],
                'Conventional Electricity Generation': ['RICE',
                                                        'Electricity Generator']
                }

        def eu_dict_to_df(eu_dict):
            """
            Convert unit type/unit name dictionaries to dataframes.
            """
            eu_df = pd.DataFrame.from_dict(
                    eu_dict, orient='index'
                    ).reset_index()

            eu_df = pd.melt(
                    eu_df, id_vars='index', value_name='unit'
                    ).rename(columns={'index': 'end_use'}).drop(
                            'variable', axis=1
                            )

            eu_df = eu_df.dropna().set_index('unit')

            return eu_df

        def eu_unit_type(unit_type, unittype_eu_df):
            """
            Match GHGRP unit type to end use specified in unittype_eu_dict.
            """

            enduse = re.match('(\w+) \(', unit_type)

            if enduse != None:

                enduse = re.match('(\w+)', enduse.group())[0]

                if enduse in unittype_eu_df.index:

                    enduse = unittype_eu_df.loc[enduse, 'end_use']

                else:

                    enduse = np.nan

            else:

                if unit_type in unittype_eu_df.index:

                    enduse = unittype_eu_df.loc[unit_type, 'end_use']

            return enduse

        def eu_unit_name(unit_name, unitname_eu_df):
            """
            Find keywords in GHGRP unit name descriptions and match them
            to appropriate end uses based on unitname_eu_dict.
            """

            for i in unitname_eu_df.index:

                enduse = re.search(i, unit_name.lower())

                if enduse == None:

                    continue

                else:

                    enduse = unitname_eu_df.loc[i, 'end_use']

                    return enduse

            enduse = np.nan

            return enduse


        unittype_eu_df = eu_dict_to_df(unittype_eu_dict)

        unitname_eu_df = eu_dict_to_df(unitname_eu_dict)

        # Base ghgrp energy end use disaggregation on reported unit type and
        # unit name.
        eu_ghgrp = self.energy_ghgrp_y.copy(deep=True)

        eu_ghgrp = eu_ghgrp[eu_ghgrp.MECS_NAICS !=0]

        # First match end uses to provided unit types. Most unit types are
        # specified as OCS (other combustion source).
        unit_types = eu_ghgrp.UNIT_TYPE.dropna().unique()

        type_match = list()

        for utype in unit_types:

            enduse = eu_unit_type(utype, unittype_eu_df)

            type_match.append([utype, enduse])

        type_match = pd.DataFrame(type_match,
                                  columns=['UNIT_TYPE', 'end_use'])

        eu_ghgrp = pd.merge(eu_ghgrp, type_match, on='UNIT_TYPE', how='left')

        # Next, match end use by unit name for facilites that report OCS for
        # unit type.
        eu_ocs = eu_ghgrp[
                (eu_ghgrp.UNIT_TYPE == 'OCS (Other combustion source)') |
                (eu_ghgrp.UNIT_TYPE.isnull())
                ][['UNIT_TYPE', 'UNIT_NAME']]

        eu_ocs['end_use'] = eu_ocs.UNIT_NAME.apply(
                lambda x: eu_unit_name(x, unitname_eu_df)
                )

        eu_ghgrp.end_use.update(eu_ocs.end_use)

        eu_ghgrp.drop(eu_ghgrp.columns.difference(
                set(['COUNTY_FIPS','MECS_Region', 'MMBtu_TOTAL', 'MECS_FT',
                     'PRIMARY_NAICS_CODE', 'MECS_NAICS','end_use',
                     'FACILITY_ID'])
                ), axis=1, inplace=True)

        # sum energy of unit types and unit names matched to an end use
        eu_ghgrp_matched = eu_ghgrp[eu_ghgrp.end_use.notnull()].pivot_table(
                values='MMBtu_TOTAL', columns='end_use',
                index=['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                       'MECS_NAICS', 'MECS_FT'], aggfunc='sum', fill_value=0
                )

        eu_ghgrp_matched = eu_ghgrp_matched.join(
                eu_ghgrp.pivot_table(values='FACILITY_ID',
                index=['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                       'MECS_NAICS', 'MECS_FT'], aggfunc='count')
                )

        # Calculate the remaining GHGRP facilities energy use
        # with MECS data.
        eu_ghgrp_notmatched = \
            eu_ghgrp[(eu_ghgrp.end_use.isnull()) &
                     (eu_ghgrp.MECS_FT.notnull())].copy(deep=True)

        enduses = eu_fraction_dict['GHGRP'].columns.values

        eu_ghgrp_notmatched = pd.merge(
                eu_ghgrp_notmatched.set_index(['MECS_NAICS', 'MECS_FT']),
                eu_fraction_dict['GHGRP'], left_index=True,
                right_index=True, how='left'
                )

        for eu in enduses:

            eu_ghgrp_notmatched[eu] = \
                eu_ghgrp_notmatched.MMBtu_TOTAL.multiply(
                        eu_ghgrp_notmatched[eu], fill_value=0
                        )

        agg_cols = [x for x in itools.product(enduses, ['sum'])]

        agg_cols.append(('FACILITY_ID', 'count'))

        eu_ghgrp_notmatched = eu_ghgrp_notmatched.reset_index().groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE', 'MECS_NAICS',
                 'MECS_FT',], as_index=False).agg(dict(agg_cols))

        eu_ghgrp_notmatched.set_index('MECS_NAICS', inplace=True)

        eu_ghgrp_matched.reset_index(
                ['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE','MECS_FT'],
                inplace=True
                )

        for df in [eu_ghgrp_matched, eu_ghgrp_notmatched]:

            df.rename(columns={'PRIMARY_NAICS_CODE':'naics',
                               'FACILITY_ID': 'est_count'}, inplace=True)

            df['Emp_Size'] = 'ghgrp'

            df['data_source'] = 'ghgrp'


        # Calculate end use of energy estimated from MECS data with MECS end
        # use.
        enduses = eu_fraction_dict['nonGHGRP'].columns.values

        eu_energy_dd = dd.merge(
                county_energy_dd[county_energy_dd.data_source=='mecs_ipf'],
                eu_fraction_dict['nonGHGRP'].reset_index('MECS_FT'),
                on=['MECS_NAICS', 'MECS_FT'], how='left'
                )

        for eu in enduses:

            eu_energy_dd[eu] = \
                eu_energy_dd.MMBtu_TOTAL.mul(eu_energy_dd[eu],
                                             fill_value=0)

        # This throws FutureWanring related to sorting for pandas concat,
        # but currently there's no option to address this in dd.concat
        eu_energy_dd = dd.concat(
                [df for df in [eu_energy_dd, eu_ghgrp_matched,
                               eu_ghgrp_notmatched]], axis='index',
                join='outer', interleave_partitions=True)


        eu_energy_dd_final = dd.melt(
                eu_energy_dd.reset_index(), value_vars=enduses.tolist(),
                id_vars=['MECS_NAICS', 'COUNTY_FIPS', 'Emp_Size', 'MECS_FT',
                         'MECS_Region', 'data_source', 'est_count', 'fipscty',
                         'fipstate', 'naics'], var_name='End_use',
                value_name='MMBtu'
                )

        # clean up by removing MMBtu values == 0..
        eu_energy_dd_final = \
            eu_energy_dd_final[eu_energy_dd_final.MMBtu !=0]

        eu_energy_dd_final = eu_energy_dd_final.set_index('MECS_NAICS')
#                                                          compute=True)

        eu_energy_dd_final['fipstate'] = eu_energy_dd_final.COUNTY_FIPS.apply(
            lambda x: int(str(x)[0:len(int(str(x)))-3])
            )

        if temps == True:

            temp_methods = enduse_temps_IPH.process_temps()

            MECS_NAICS = eu_fraction_dict[
                    'nonGHGRP'
                    ].index.get_level_values(0).unique().values

            eu_energy_dd_final_temps = temp_methods.temp_mapping(MECS_NAICS,
                                                           eu_energy_dd_final)

            return eu_energy_dd_final_temps


        else:

            return eu_energy_dd_final



    ##
    # #Results analysis
    # with pd.ExcelWriter('2010_comparisons.xlsx') as writer:
    #   CountyEnergy.groupby('MECS_Region').sum().to_excel(
    #       writer, sheet_name = 'By Region'
    #       )
    #   CountyEnergy_wGHGRP.groupby('MECS_Region').sum().to_excel(
    #       writer, sheet_name = 'By Region wGHGRP'
    #       )
    #   CountyEnergy.groupby('MECS_NAICS').sum().to_excel(
    #       writer, sheet_name = 'By NAICS'
    #       )
    #   CountyEnergy_wGHGRP.groupby('MECS_NAICS').sum().to_excel(
    #       writer, sheet_name = 'By NAICS wGHGRP'
    #       )
    #   CountyEnergy.groupby(('MECS_Region', 'MECS_NAICS')).sum().to_excel(
    #       writer, sheet_name = 'By Region & NAICS'
    #       )
    #   CountyEnergy_wGHGRP.groupby(('MECS_Region', 'MECS_NAICS')).sum().to_excel(
    #       writer, sheet_name = 'By Region & NAICS wGHGRP'
    #       )
    #
