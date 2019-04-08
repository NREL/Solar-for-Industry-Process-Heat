import pandas as pd
import numpy as np
import itertools as itools
import os

class Manufacturing_energy:

    #Set analysis year and required file paths
    def __init__(self, year, energy_ghgrp):

        self.year = year
        
        self.file_dir = '../calculation_data'

        self.fuelxwalk_file = 'MECS_FT_IPF.csv'
        
        self.naics_2012_file = 'mecs_naics_2012.csv'
        
        self.naics_old_file = 'mecs_naics.csv'
        
        self.ipf_results_file = 'mecs_'+str(self.year)+\
            'ipf_results_naics_employment_csv'
        
        self.mecs_naics = pd.DataFrame()
        
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
        #First add column of CBP-Matched NAICS, 'NAICS_USED'
        self.energy_ghgrp_y = \
            pd.merge(self.energy_ghgrp_y,
                     ghgrp_matching[['FACILITY_ID', 'NAICS_USED']],
                     on='FACILITY_ID', how='left')

        self.energy_ghgrp_y = MatchMECS_NAICS(
                self.energy_ghgrp_y, 'NAICS_USED'
                )
        
#        return self.energy_ghgrp_y


    def GHGRP_Totals_byMECS(self):
        """
        From caclualted GHGRP energy data, create sums by MECS Region, 
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

    def GHGRP_electricity_calc(GHGRP_electricity, cbp_matching):
        """
        Requires running format_eia923() from EIA_CHP.py
        """

        EIA923_2014counts = pd.DataFrame(
            GHGRP_electricity.groupby('FIPS_NAICS')['FACILITY_ID'].count()
            )

        EIA923_2014counts.rename(
            columns={'FACILITY_ID':'fac_count923'}, inplace=True
            )

        EIA923_2014counts.loc[:, 'FIPS_NAICS'] = EIA923_2014counts.index.values

        EIA923_2014counts = EIA923_2014counts.merge(
            cbp_matching[['ghgrp_fac', 'fips_n']], left_index=True, 
                right_on = 'fips_n'
            )

        # Create new corrections of CBP facility counts where the number of
        # EIA923 facilities != number of GHGRP facilities.
        fips_naics_923 = EIA923_2014counts[(
            EIA923_2014counts.fac_count923 != EIA923_2014counts.ghgrp_fac
            )]

        cbp_formatching_923 = pd.merge(cbp_matching, fips_naics_923[
                ['fac_count923', 'fips_n']], on='fips_n'
            )

        # The following should be made into a method based on method in 
        # Match_GHGRP_County.py.
        large = ['n50_99', 'n100_249', 'n250_499', 'n500_999', 'n1000']

        small = ['n1_4', 'n5_9', 'n10_19', 'n20_49']

        for i in cbp_formatching_923.index:
            if cbp_formatching_923.loc[i,'fac_count923'] > cbp_formatching_923.loc[
                i,'est']:
            
                count = cbp_formatching_923.loc[i, 'est']

            else:
                count = cbp_formatching_923.loc[i, 'fac_count923']

            while count > 0:
                maxsize = [c for c in itools.compress(small + large, 
                    cbp_formatching_923.ix[i, ('n1_4'):('n1000')].values
                    )][-1]

                cbp_formatching_923.loc[i, maxsize] = cbp_formatching_923.loc[
                    i, maxsize] - 1

                count = count - 1
                
            cbp_formatching_923.loc[i, 'est_large_corrected'] = \
                cbp_formatching_923.loc[i, ('n50_99'):('n1000')].sum()

            cbp_formatching_923.loc[i, 'est_small_corrected'] = \
                cbp_formatching_923.loc[i, ('n1_4'):('n20_49')].sum()

        cbp_formatching_923.loc[:, 'n1_49'] = cbp_formatching_923[[
            'n1_4', 'n5_9', 'n10_19', 'n20_49'
            ]].sum(axis=1)

        # Reindex to match corresponding cbp_matching index values
        cbp_formatching_923.loc[:, 'cbpfm_i']  = [
            cbp_matching[cbp_matching.fips_n == n].index[0] for n in \
                cbp_formatching_923.fips_n
            ]   

        cbp_formatching_923.set_index(['cbpfm_i'], inplace=True)

        cbp_corrected_923 = pd.DataFrame(cbp_matching, copy=True)

        cbp_corrected_923.update(cbp_formatching_923)

        return cbp_corrected_923

    def format_IPF(self):
        """
        Format results from IPF of MECS energy data by region, fuel type,
        and employment size.
        """

        ipf_results_formatted = pd.read_csv(
                os.path.join(self.file_dir, self.ipf_results_file)
                )
        
        ipf_results_formatted = ipf_results_formatted.T
        
        ipf_results_formatted.columns = [int(x) for x in 
            ipf_results_formatted.loc['naics', :].values]
            
        ipf_results_formatted.drop('naics', axis=0, inplace=True)

        ipf_results_formatted["MECS_FT"] = [
            x[x.find("_") + 1 : x.rfind("_")] for x \
                in list( ipf_results_formatted.index)
            ]

        ipf_results_formatted["MECS_Region"] = [
            x[0 : x.find("_")] for x in list(ipf_results_formatted.index)
            ]

        ipf_results_formatted["Emp_Size"] = [
            x[x.rfind("_") + 1 : len(x)] for x in list(
                ipf_results_formatted.index
                )
            ]

        ipf_results_formatted.loc[:, 'Emp_Size'] = \
            ipf_results_formatted['Emp_Size'].map(self.empsize_dict)

        return ipf_results_formatted


    def calc_intensities(self, cbp_matching):
        """
        Calculate MECS intensities (energy per establishment) based on 2010 or
        2014 CBP establishment counts.
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
                os.path.join(self.file_dir, self.ipf_results_file)
                )
        
        ipf_results_formatted = ipf_results_formatted.T
        
        ipf_results_formatted.columns = [int(x) for x in 
            ipf_results_formatted.loc['naics', :].values]
            
        ipf_results_formatted.drop('naics', axis=0, inplace=True)

        ipf_results_formatted["MECS_FT"] = [
            x[x.find("_") + 1 : x.rfind("_")] for x \
                in list( ipf_results_formatted.index)
            ]

        ipf_results_formatted["MECS_Region"] = [
            x[0 : x.find("_")] for x in list(ipf_results_formatted.index)
            ]

        ipf_results_formatted["Emp_Size"] = [
            x[x.rfind("_") + 1 : len(x)] for x in list(
                ipf_results_formatted.index
                )
            ]

        ipf_results_formatted.loc[:, 'Emp_Size'] = \
            ipf_results_formatted['Emp_Size'].map(self.empsize_dict)

        MECS_intensities = pd.DataFrame(
            ipf_results_formatted.values, index=ipf_results_formatted.index.values,
            columns=ipf_results_formatted.columns
            )

        MECS_calc = pd.DataFrame(
            ipf_results_formatted.values, index=ipf_results_formatted.index.values,
            columns=ipf_results_formatted.columns
            )

        MECS_intensities.iloc[:, 0:81] = 0

        MECS_calc.iloc[:, 0:81] = 0

        for r in MECS_intensities.MECS_Region.unique():
            
            for s in self.empsize_dict.values():
                
                rs_index = MECS_intensities[
                    (MECS_intensities.MECS_Region == r) &
                        (MECS_intensities.Emp_Size == s)
                    ].index

                MECS_intensities.loc[rs_index, MECS_intensities.columns[0:81]] = \
                    ipf_results_formatted.loc[
                        rs_index, ipf_results_formatted.columns[0:81]
                            ] / cbp_matching[(cbp_matching.MECS_NAICS != 0) & (
                                cbp_matching.MECS_Region == r
                                    )].groupby('MECS_NAICS').sum()[s].T

                MECS_calc.loc[rs_index, MECS_calc.columns[0:81]] = \
                    MECS_intensities.loc[
                        rs_index, MECS_intensities.columns[0:81]
                            ] * cbp_matching[(cbp_matching.MECS_NAICS != 0) & (
                                cbp_matching.MECS_Region == r
                                    )].groupby('MECS_NAICS').sum()[s].T         
            
        #Record the NAICS and regions where IPF calculates MECS fuel use, but 
        #CBP records no facilities of that NAICS (e.g., 'NaN' values)
        #Used to develop original seed for IPF calculations.
        MECS_no_CBPfacility = pd.DataFrame(
            MECS_intensities.iloc[:,0:81] != np.inf
            )

        MECS_no_CBPfacility[
            MECS_no_CBPfacility == False].fillna(1).to_csv(self.IPF_feed_file)

        #Fill NaN values for intensities with 0.
        MECS_intensities.fillna(0, inplace=True)

        MECS_intensities.replace(np.inf, 0, inplace=True)

        #Create tuples of fuel type and employment size for future matching
        MECS_intensities["FT_Emp"] = [
            z for z in zip(
                MECS_intensities.MECS_FT.values, \
                    MECS_intensities.Emp_Size.values
                )
            ]

        return MECS_intensities


    @staticmethod
    def combfuel_calc(cbp_corrected, MECS_intensities):

        """
        Calculate county-level manufacturing energy use based on CBP facility 
        counts, calculated MECS intensities, and calculated facility energy use 
        for GHGRP facilites.
        Net electricity undergoes an additional adjustment.
        """

        CountyEnergy_wGHGRP = pd.DataFrame(
            cbp_corrected[cbp_corrected.MECS_NAICS.notnull()],
            index = cbp_corrected[cbp_corrected.MECS_NAICS.notnull()].index,
            columns = ['fipstate', 'fipscty', 'fips_matching','naics',
                'MECS_NAICS', 'MECS_Region']
            )

        CountyEnergy_wGHGRP.loc[:, 'fips_matching'] = [
            int(x) for x in CountyEnergy_wGHGRP.fips_matching
            ]

        #Net electricity is calculated separately in the elec_calc method.
        for FT in MECS_intensities[
            MECS_intensities.MECS_FT !='Net_electricity'
            ]['MECS_FT'].drop_duplicates():

            r_df = pd.DataFrame(
                index=CountyEnergy_wGHGRP.index, columns=list(
                    CountyEnergy_wGHGRP.MECS_Region.drop_duplicates()
                    )
                )

            for r in CountyEnergy_wGHGRP.MECS_Region.drop_duplicates():
                
                fuel_df = pd.DataFrame(index=CountyEnergy_wGHGRP.index)

                for n in CountyEnergy_wGHGRP.MECS_NAICS.drop_duplicates():
                    cbpi = CountyEnergy_wGHGRP[
                        (CountyEnergy_wGHGRP.MECS_Region == r) & \
                            (CountyEnergy_wGHGRP.MECS_NAICS == n)
                        ].index
                    
                    fuel_sum = pd.DataFrame(index = cbpi)

                    for s in MECS_intensities.Emp_Size.drop_duplicates()[0:6]:
                        fuel_sum.loc[:,s] = MECS_intensities[
                            (MECS_intensities.MECS_Region == r) & \
                                (MECS_intensities.FT_Emp == (FT,s))
                            ][n].values[0] * cbp_corrected.loc[cbpi,s]

                    fuel_df = pd.concat(
                        [fuel_df, fuel_sum.sum(axis=1)], axis=1, \
                            join='outer'
                        )

                r_df[r] = fuel_df.sum(axis=1)

            CountyEnergy_wGHGRP.loc[:, FT] = r_df.sum(axis=1)

        CountyEnergy_wGHGRP.loc[:, 'naics'] = [
            int(x) for x in CountyEnergy_wGHGRP.naics
            ]

        return CountyEnergy_wGHGRP  

    
    def final_merging(self, ghgrp_matching, CountyEnergy_wGHGRP):
        """
        Method for merging enegy values calculated from GHGRP and from
        MECS intensities. Includes mining industries.
        """

        mfg = pd.DataFrame(
            ghgrp_matching[
                (ghgrp_matching.NAICS_USED.between(310000, 340000)) & 
                (ghgrp_matching.COUNTY_FIPS.between(0, 72000, 
                                                        inclusive=False)) 
                ].loc[:, ['COUNTY_FIPS', 'FACILITY_ID', 'NAICS_USED']]
            )

        mfg_grouped = self.energy_ghgrp_y[ 
            (self.energy_ghgrp_y.NAICS_USED > 310000) &
            (self.energy_ghgrp_y.MECS_FT.notnull())
            ].groupby(('FACILITY_ID', 'MECS_FT'))

        #Drop GHGRP entry with FIPS = 0 and FIPS > 56 
        #(i.e., territories like VI, PR)
        mfg = pd.DataFrame(
            mfg[mfg.COUNTY_FIPS.between(0, 57000, inclusive=False)]
            )

        def group_energy_calc(df, df_grouped):
            """
            Calculate energy in TBtu.
            """
            for group in df_grouped.groups:
                df.loc[
                    df[df.FACILITY_ID == group[0]].index,
                        group[1]
                    ] = \
                        df_grouped.get_group(group)[
                            'MMBtu_TOTAL'
                            ].sum() / 1000000

            return df

        mfg = group_energy_calc(mfg, mfg_grouped)

        #Create FIPS-NAICS tuples to match GHGRP with county-level 
        #energy calcualted from MECS.
        CountyEnergy_wGHGRP['FIPS_NAICS'] = [
            z for z in zip(
                CountyEnergy_wGHGRP.fips_matching.values,
                    CountyEnergy_wGHGRP.naics.values
                )
            ]

        CountyEnergy_wGHGRP.set_index('FIPS_NAICS', drop=True, inplace=True)

        for s in [mining, mfg]:
            s['NAICS_USED'] = [int(x) for x in s.NAICS_USED]

            s['FIPS_NAICS'] = [z for z in zip(
                s.COUNTY_FIPS.values, s.NAICS_USED.values
                )]

            s.set_index('FIPS_NAICS', drop=False, inplace=True)

        mfg['in_CBP'] = [x in CountyEnergy_wGHGRP.index for x in mfg.index]    

        mining_add = pd.DataFrame()

        fuel_types = ['Diesel', 'Natural_gas', 'Residual_fuel_oil', 'Other',
            'Coal', 'Coke_and_breeze', 'LPG_NGL']

        for FT in fuel_types:

            if FT in mfg.columns:
                #First sum county energy by fuel type and NAICS for 
                #GHGRP facilities with a CBP match
                CountyEnergy_wGHGRP.loc[:,FT] = CountyEnergy_wGHGRP[FT].add(
                    mfg.groupby(mfg.index)[FT].sum(), axis='index',
                        fill_value=0
                    )

            if FT in mining.columns:
                mining_add = pd.concat(
                    [mining_add, mining.groupby(mining.index)[FT].sum()],
                    axis=1
                    )

            else:
                pass 
        
        # Create dataframe with GHGRP facilities that do not have CBP matches.
        mfg_add = mfg[mfg.in_CBP == False].groupby(
            mfg[mfg.in_CBP == False].index
            )[fuel_types].sum()

        # Format and append missing mining and manufacturing energy. 
        for df in [mfg_add, mining_add]:

            df['fips_matching'] = [l[0] for l in df.index]

            df['fipscty'] = [int(str(x)[2:]) for x in df.fips_matching]

            df['fipstate'] = [
                int(str(x)[0:(len(str(x))-3)]) for x in df.fips_matching
                ] 

            df['naics'] = [l[1] for l in df.index]

            CountyEnergy_wGHGRP = CountyEnergy_wGHGRP.append(df)


        return CountyEnergy_wGHGRP


    def elec_calc(GHGRP_electricity, CountyEnergy_wGHGRP, cbp_corrected_923, \
        MECS_intensities):
        """Calculate net electricity based on EIA 923 data. First use values
        calculated prior to correcting for GHGRP facilities.
        """

        GHGRP_electricity.set_index(['FIPS_NAICS'], drop=True, inplace=True)

        r_df = pd.DataFrame(
            index=cbp_corrected_923.dropna(subset=['MECS_NAICS']).index, \
                columns=list(cbp_corrected_923.MECS_Region.drop_duplicates()
                )
            )

        for r in cbp_corrected_923.MECS_Region.drop_duplicates():
                
            fuel_df = pd.DataFrame(
                index = \
                    cbp_corrected_923.dropna(subset=['MECS_NAICS']).index,\
                columns=['Net_electricity']
                )

            for n in cbp_corrected_923.loc[
                fuel_df.index, 'MECS_NAICS'].drop_duplicates():

                cbpi = \
                    cbp_corrected_923[(cbp_corrected_923.MECS_Region == r) & \
                        (cbp_corrected_923.MECS_NAICS == n)].index
                
                fuel_sum = pd.DataFrame(index=cbpi)

                for s in MECS_intensities.Emp_Size.drop_duplicates()[0:6]:
                    fuel_sum.loc[:,s] = MECS_intensities[
                        (MECS_intensities.MECS_Region == r) & \
                            (MECS_intensities.FT_Emp == ('Net_electricity',s))
                        ][n].values[0] * cbp_corrected_923.loc[cbpi,s]

                fuel_sum = pd.DataFrame(
                    fuel_sum.sum(axis=1), columns=['Net_electricity']
                    )

                fuel_df.update(fuel_sum, overwrite=True)

                # fuel_df = pd.concat(
                #    [fuel_df, fuel_sum.sum(axis = 1)], axis = 1, join = 'outer'
                #    )

            r_df[r] = fuel_df.sum(axis=1)

        r_df.loc[:, 'FIPS_NAICS'] = cbp_corrected_923.dropna(
            subset = ['MECS_NAICS']).fips_n

        r_df.set_index(['FIPS_NAICS'], drop=True, inplace=True)

        #Add column for electricity reported on Form EIA-923
        r_df['elec923'] = GHGRP_electricity.groupby(
            GHGRP_electricity.index
            ).Net_electricity.sum()

        CountyEnergy_wGHGRP.loc[r_df.index, 'Net_electricity'] = r_df.sum(
            axis=1
            )

        return CountyEnergy_wGHGRP

    # for df in [CountyEnergy, CountyEnergy_wGHGRP]:
    #   df['Total'] = df[[
    #       'Net_electricity', 'Residual_fuel_oil', 'Diesel', 'Natural_gas', \
    #       'LPG_NGL', 'Coal', 'Coke_and_breeze', 'Other'
    #       ]].sum(axis = 1)

    #ctyavg = np.mean(
            # final_mfg_energy.groupby('fips_matching')['Total'].sum()
            # )

    # ctyavg = np.mean(
    #     CountyEnergy.groupby('fips_matching')['Total'].sum()
    #     )

    # final_mfg_energy.groupby('fips_matching')['Total'].apply(
    #     lambda x: np.sum(x) - ctyavg_wGHGRP).to_csv('CountyEnergy_compare.csv')

    ###
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