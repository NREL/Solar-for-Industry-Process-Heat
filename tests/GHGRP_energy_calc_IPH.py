# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 14:04:47 2017
@Colin McMillan, colin.mcmillan@nrel.gov
"""

import pandas as pd
import numpy as np
import ghg_tiers
import requests
import os
import find_fips
import ghg_tiers_IPH
import Get_GHGRP_data_IPH
import GHGRP_energy_calc_IPH
import GHGRP_AAenergy_calc_IPH

class GHGRP_calcs:
    
    def __init__(self, years):
        
        if type(years) == tuple:
            
            self.years = range(years[0], years[0])
            
        else:
            
            self.years = [years]

        self.table_dict = {'subpartC': 'C_FUEL_LEVEL_INFORMATION',
                           'subpartD': 'D_FUEL_LEVEL_INFORMATION',
                           'subpartV_fac': 'V_GHG_EMITTER_FACILITIES', 
                           'subpartV_emis': 'V_GHG_EMITTER_SUBPART',
                           'subpartAA_ff':'AA_FOSSIL_FUEL_INFORMATION', 
                           'subpartAA_liq': 'AA_SPENT_LIQUOR_INFORMATION'}
        
        self.tier_data_columns = ['FACILITY_ID', 'REPORTING_YEAR',
                                  'FACILITY_NAME','UNIT_NAME', 'UNIT_TYPE',
                                  'FUEL_TYPE','FUEL_TYPE_OTHER',
                                  'FUEL_TYPE_BLEND']
        
        # Set calculation data directories
        self.file_dir = os.path.join('../', 'calculation_data')
        
        ## Set GHGRP data file directory
        self.ghgrp_file_dir = \
            os.path.join('../','calculation_data/ghgrp_data/')

        # List of facilities for correction of combustion emissions from Wood 
        #and Wood Residuals for using Subpart C Tier 4 calculation methodology.
        self.wood_facID = pd.read_csv(
                self.file_dir + '/WoodRes_correction_facilities.csv',
                index_col=['FACILITY_ID']
                )
        
        self.std_efs = pd.read_csv(self.file_dir + '/EPA_FuelEFs.csv',
                                   index_col = ['Fuel_Type'])
        
        self.std_efs.index.name = 'FUEL_TYPE'
        
        self.ghg_tiers = ghg_tiers_IPH.tier_energy(years=self.years)
        
        self.MECS_regions = pd.read_csv(
                self.file_dir+'/US_FIPS_Codes.csv', index_col=['COUNTY_FIPS']
                )
        
        self.fac_file_2010 = pd.read_csv(
                self.ghgrp_file_dir+'fac_table_2010.csv', encoding='latin_1'
                )

    # Get data from EPA API if not available
    def import_data(self, subpart):
        """
        Download EPA data via API if emissions data are not saved locally.
        """

        def download_or_read_ghgrp_file(subpart, filename):
            """
            Method for checking for saved file or calling download method
            for all years in instantiated class.
            """

            ghgrp_data = pd.DataFrame()
            
            table = self.table_dict[subpart]
            
            for y in self.years:
            
                filename = filename + str(y) + '.csv'
        
                if filename not in os.listdir(self.ghgrp_file_dir):
                    
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                        
                    data_y.to_csv(self.ghgrp_file_dir  + filename + '.csv')
                    
                else:
                    
                    data_y = pd.read_csv(self.ghgrp_file_dir + filename)
                    
                ghgrp_data = ghgrp_data.append(data_y, ignore_index=True)
                
            return ghgrp_data

        if subpart in ['subpartC', 'subpartD']:
            
            filename = self.table_dict[subpart][0:7].lower()
            
            ghgrp_data = download_or_read_ghgrp_file(subpart, filename)
            
            formatted_ghgrp_data = format_GHGRP_emissions(ghgrp_data)
            
        if subpart == 'subpartV_fac':
            
            filename = 'fac_table_'
            
            ghgrp_data = download_or_read_ghgrp_file(subpart, filename)

            formatted_ghgrp_data = format_GHGRP_facilities(ghgrp_data)
            
        else:
            
            if subpart == 'subpartV_emis':
            
                filename = 'V_GHGs_'
            
            if subpart == 'subpartAA_ff':
                
                filename = 'aa_ffuel_'
                
            if subpart == 'subpartAA_liq':
                
                filename = 'aa_sl_'    

            formatted_ghgrp_data = \
                download_or_read_ghgrp_file(subpart, filename)

        return formatted_ghgrp_data

    def format_GHGRP_emissions(self, GHGs):
        """
        Format and correct for odd issues with reported data in subparts C
        and D .
        """
    
        GHGs.dropna(axis=0, subset=['FACILITY_ID'], inplace=True)
    
        for c in ('FACILITY_ID', 'REPORTING_YEAR'):
    
            GHGs.loc[:, c] = [int(x) for x in GHGs[c]]
    
        #Adjust multiple reporting of fuel types
        fuel_fix_index = GHGs[(GHGs.FUEL_TYPE.notnull() == True) & 
            (GHGs.FUEL_TYPE_OTHER.notnull() == True)].index
    
        GHGs.loc[fuel_fix_index, 'FUEL_TYPE_OTHER'] = np.nan
    
        # Fix errors in reported data.
        if 2014 in self.years:
            
            for i in list(GHGs[(GHGs.FACILITY_ID == 1005675) & \
                (GHGs.REPORTING_YEAR == 2014)].index):
    
                GHGs.loc[i, 'TIER2_CH4_EMISSIONS_CO2E'] = \
                    GHGs.loc[i, 'TIER2_CH4_COMBUSTION_EMISSIONS'] * 25.135135
    
                GHGs.loc[i, 'TIER2_N2O_EMISSIONS_CO2E'] = \
                    GHGs.loc[i, 'TIER2_N2O_COMBUSTION_EMISSIONS'] * 300
    
            for i in GHGs[(GHGs.FACILITY_ID == 1001143) & \
                (GHGs.REPORTING_YEAR == 2014)].index:
        
        	        GHGs.loc[i, 'T4CH4COMBUSTIONEMISSIONS'] = \
                    GHGs.loc[i, 'T4CH4COMBUSTIONEMISSIONS']/1000
        
        	        GHGs.loc[i, 'T4N2OCOMBUSTIONEMISSIONS'] = \
                    GHGs.loc[i, 'T4N2OCOMBUSTIONEMISSIONS']/1000
        
        if 2012 in self.years:
            
            selection = GHGs.loc[(GHGs.FACILITY_ID == 1000415) &
                                 (GHGs.FUEL_TYPE == 'Bituminous') &
                                 (GHGs.REPORTING_YEAR == 2012)].index
                
            GHGs.loc[selection, 
                     ('T4CH4COMBUSTIONEMISSIONS'):('TIER4_N2O_EMISSIONS_CO2E')
                     ] = GHGs.loc[selection, 
                         ('T4CH4COMBUSTIONEMISSIONS'):('TIER4_N2O_EMISSIONS_CO2E')
                         ] / 10
    
        GHGs.loc[:, 'CO2e_TOTAL'] = 0
        
        total_co2 = pd.DataFrame()
     
        for tier in ['TIER1_', 'TIER2_', 'TIER3_']:
    
            for ghg in ['CH4_EMISSIONS_CO2E', 'N2O_EMISSIONS_CO2E', \
                'CO2_COMBUSTION_EMISSIONS']:
                
                total_co2 = pd.concat([total_co2, GHGs[tier + ghg]], axis=1)
    
        for ghg in ['TIER4_CH4_EMISSIONS_CO2E', 'TIER4_N2O_EMISSIONS_CO2E']:
    
            total_co2 = pd.concat([total_co2, GHGs[ghg]], axis=1)
            
        total_co2.fillna(0, inplace=True)
        
        GHGs.loc[:, 'CO2e_TOTAL'] = total_co2.sum(axis=1)

        return GHGs


    def format_GHGRP_facilities(self, oth_facfile):
        """
        Format csv file of facility information. Requires list of facility
        files for 2010 and for subsequent years.
        Assumes 2010 file has the correct NAICS code for each facilitiy;
        subsequent years default to the code of the first year a facility
        reports.
        """
    
        def fac_read_fix(ffile):
            """
            Reads and formats facility csv file.
            """
    
    #        Duplicate entries in facility data query. Remove them to enable a 1:1
    #        mapping of facility info with ghg data via FACILITY_ID.
    #        First ID facilities that have cogen units.
            fac_cogen = facdata.FACILITY_ID[
                facdata['COGENERATION_UNIT_EMISS_IND'] == 'Y'
                ]
    
            #facdata.drop_duplicates('FACILITY_ID', inplace=True)
    
            facdata.dropna(subset=['FACILITY_ID'], inplace=True)
    
            #Reindex dataframe based on facility ID
            facdata.FACILITY_ID = facdata.FACILITY_ID.apply(np.int)
    
            #Correct PRIMARY_NAICS_CODE from 561210 to 324110 for Sunoco Toledo 
            #Refinery (FACILITY_ID == 1001056); correct PRIMARY_NAICS_CODE from 
            #331111 to 324199 for Mountain State Carbon, etc.
            fix_dict = {1001056: {'PRIMARY_NAICS_CODE': 324110},
                        1001563: {'PRIMARY_NAICS_CODE': 324119},
                        1006761: {'PRIMARY_NAICS_CODE': 331221},
                        1001870: {'PRIMARY_NAICS_CODE': 325110},
                        1006907: {'PRIMARY_NAICS_CODE': 424710},
                        1006585: {'PRIMARY_NAICS_CODE': 324199},
                        1002342: {'PRIMARY_NAICS_CODE': 325222},
                        1002854: {'PRIMARY_NAICS_CODE': 322121},
                        1007512: {'SECONDARY_NAICS_CODE': 325199},
                        1004492: {'PRIMARY_NAICS_CODE': 541712},
                        1002434: {'PRIMARY_NAICS_CODE': 322121,
                                  'SECONDARY_NAICS_CODE': 322222},
                        1002440: {'SECONDARY_NAICS_CODE': 221210,
                                  'PRIMARY_NAICS_CODE': 325311},
                        1003006: {'PRIMARY_NAICS_CODE': 324110}}
    
            for k, v in fix_dict.items():

                facdata.loc[facdata[facdata.FACILITY_ID == k].index, 
                            list(v)[0]] = list(v.values())[0]
    
            facdata.set_index(['FACILITY_ID'], inplace=True)
    
    #        Re-label facilities with cogen units
            facdata.loc[fac_cogen, 'COGENERATION_UNIT_EMISS_IND'] = 'Y'
    
            facdata['MECS_Region'] = ""
    
            return facdata
    
        all_fac = fac_read_fix(self.fac_file_2010)
    
        all_fac = all_fac.append(fac_read_fix(oth_facfile))
    
    #    Drop duplicated facility IDs, keeping first instance (i.e., year).
        all_fac = pd.DataFrame(all_fac[~all_fac.index.duplicated(keep='first')])
    
    #    Identify facilities with missing County FIPS data and fill missing data.
    #    Most of these facilities are mines or natural gas/crude oil processing
    #    plants.
        ff_index = all_fac[all_fac.COUNTY_FIPS.isnull() == False].index
    
        all_fac.loc[ff_index, 'COUNTY_FIPS'] = \
            [np.int(x) for x in all_fac.loc[ff_index, 'COUNTY_FIPS']]
    
    #    Update facility information with new county FIPS data
        missingfips = pd.DataFrame(all_fac[all_fac.COUNTY_FIPS.isnull() == True])
    
        missingfips.loc[:, 'COUNTY_FIPS'] = \
            [find_fips.fipfind(i, missingfips) for i in missingfips.index]
    
        all_fac.loc[missingfips.index, 'COUNTY_FIPS'] = missingfips.COUNTY_FIPS
    
        all_fac['COUNTY_FIPS'].fillna(0, inplace=True)
    
    #    Assign MECS regions and NAICS codes to facilities and merge location data
    #    with GHGs dataframe.
    #    EPA data for some facilities are missing county fips info
        all_fac.COUNTY_FIPS = all_fac.COUNTY_FIPS.apply(np.int)
    
        concat_mecs_region = \
            pd.concat(
                [all_fac.MECS_Region, self.MECS_regions.MECS_Region], axis=1, \
                    join_axes=[all_fac.COUNTY_FIPS]
                )
    
        all_fac.loc[:,'MECS_Region'] = concat_mecs_region.iloc[:, 1].values
    
        all_fac.rename(columns = {'YEAR': 'FIRST_YEAR_REPORTED'}, inplace=True)
        
        all_fac.reset_index(drop=False, inplace=True)
    
        return all_fac


    def MMBTU_calc_CO2(self, GHGs, c):
        """
        Calculate MMBtu value based on reported CO2 emissions.
        Does not capture emissions and energy from facilities using Tier 4
        calculation methodology.
        """
        emissions = GHGs[c].fillna(0)*1000
    
        name = GHGs[c].name[0:5]+'_MMBtu'
    
        df_energy = pd.DataFrame()
    
        for fuel_column in ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']:
    
            fuel_index = GHGs.loc[emissions.index, fuel_column].dropna().index
    
            mmbtu = emissions.loc[fuel_index] / \
                GHGs.loc[fuel_index, fuel_column].map(
                        self.std_efs['CO2_kgCO2_per_mmBtu']
                        )
    
            df_energy = pd.concat([df_energy, mmbtu], axis=0)
    
        df_energy.columns = [name]
    
        df_energy.sort_index(inplace=True)
    
        return df_energy

    
    def MMBTU_calc_CH4(self, GHGs, c):
        """
        Calculate MMBtu value based on CH4 emissions reported under Tier 4.
        """
        emissions = GHGs[c].fillna(0) * 1000000
    
        name = GHGs[c].name[0:5] + '_MMBtu'
    
        df_energy = pd.DataFrame()
    
        for fuel_columns in ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']:
    
            fuel_index = GHGs.loc[emissions.index, fuel_columns].dropna().index
    
            mmbtu = emissions.loc[fuel_index] / \
                GHGs.loc[fuel_index, fuel_columns].map(
                        self.std_efs['CH4_gCH4_per_mmBtu']
                        )
    
            df_energy = pd.concat([df_energy, mmbtu], axis=0)
    
        df_energy.columns = [name]
    
        df_energy.sort_index(inplace=True)
    
        return df_energy
    
    def calculate_energy(self, GHGs, all_fac):
        """
        Apply MMBTU_calc_CO2 function to EPA emissions table Tier 1, Tier 2, and
        Tier 3 emissions; MMBTU_calc_CH4 for to Tier 4 CH4 emissions.
        Adds heat content of fuels reported under 40 CFR Part 75 (electricity
        generating units and other combustion sources covered under EPA's
        Acid Rain Program).
        """
        merge_cols = list(all_fac.columns.difference(GHGs.columns))
    
        merge_cols.append('FACILITY_ID')
    
        GHGs = pd.merge(
            GHGs, all_fac[merge_cols], how='inner', on='FACILITY_ID'
            )
    
    #   First, zero out 40 CFR Part 75 energy use for electric utilities
        GHGs.loc[GHGs[GHGs.PRIMARY_NAICS_CODE == 221112].index,
            'TOTAL_ANNUAL_HEAT_INPUT'] = 0
    
        GHGs.loc[GHGs[GHGs.PRIMARY_NAICS_CODE == 221112].index,
            'PART_75_ANNUAL_HEAT_INPUT'] = 0
    
        # Correct for revision in 2013 to Table AA-1 emission factors for kraft 
        # pulping liquor emissions. CH4 changed from 7.2g CH4/MMBtu HHV to 
        # 1.9g CH4/MMBtu HHV.           
        GHGs.loc[:, 'wood_correction'] = \
            [x in self.wood_facID.index for x in GHGs.FACILITY_ID] and 
            [f == 'Wood and Wood Residuals' for f in GHGs.FUEL_TYPE] and 
            [GHGs.REPORTING_YEAR.isin([2010, 2011, 2012])]

        GHGs.loc[(GHGs.wood_correction == True), 'T4CH4COMBUSTIONEMISSIONS'] =\
            GHGs.loc[
                (GHGs.wood_correction == True), 'T4CH4COMBUSTIONEMISSIONS'
                ].multiply(1.9 / 7.2)    
    
        # Separate, additional correction for facilities appearing to have 
        # continued reporting with previous CH4 emission factor for kraft liquor
        #combusion (now reported as Wood and Wood Residuals (dry basis).
        wood_fac_add = [1001892, 1005123, 1006366, 1004396]

        GHGs.loc[:, 'wood_correction_add'] = \
                [x in wood_fac_add for x in GHGs.FACILITY_ID] and 
                [GHGs.REPORTING_YEAR.isin([2013])]

        GHGs.loc[(GHGs.wood_correction_add == True) & 
            (GHGs.FUEL_TYPE == 'Wood and Wood Residuals (dry basis)'), 
                'T4CH4COMBUSTIONEMISSIONS'] =\
                GHGs.loc[(GHGs.wood_correction_add == True) & 
                    (GHGs.FUEL_TYPE == 'Wood and Wood Residuals (dry basis)'), 
                        'T4CH4COMBUSTIONEMISSIONS'].multiply(1.9 / 7.2)    
    
        tier_calcs = ghg_tiers.tier_energy()
        
        #New method for calculating energy based on tier methodology
        df_mmbtu_CO2 = tier_calcs.calc_all_tiers(GHGs)
        
    #    co2columns = \
    #        ['TIER1_CO2_COMBUSTION_EMISSIONS', 'TIER2_CO2_COMBUSTION_EMISSIONS',
    #        'TIER3_CO2_COMBUSTION_EMISSIONS']
    #
    #    df_mmbtu_CO2 = pd.concat(
    #        [MMBTU_calc_CO2(GHGs, c, EFs) for c in co2columns],  axis=1
    #        )
    
        df_mmbtu_CH4 = \
            pd.DataFrame(MMBTU_calc_CH4(GHGs, 'T4CH4COMBUSTIONEMISSIONS', EFs))
    
        df_mmbtu_all = pd.concat([df_mmbtu_CO2, df_mmbtu_CH4], axis=1)
    
        df_mmbtu_all.fillna(0, inplace=True)
    
        df_mmbtu_all.loc[:, 'TOTAL'] = df_mmbtu_all.sum(axis=1)
    
        GHGs.PART_75_ANNUAL_HEAT_INPUT.fillna(0, inplace=True)
    
        GHGs.loc[:, 'MMBtu_TOTAL'] = 0
    
        GHGs.MMBtu_TOTAL = df_mmbtu_all.TOTAL
    
        GHGs.loc[:, 'MMBtu_TOTAL'] = \
            GHGs[['MMBtu_TOTAL', 'PART_75_ANNUAL_HEAT_INPUT',
            'TOTAL_ANNUAL_HEAT_INPUT']].sum(axis=1)
    
        GHGs.loc[:, 'GWh_TOTAL'] = GHGs.loc[:, 'MMBtu_TOTAL']/3412.14
    
        GHGs.loc[:, 'TJ_TOTAL'] = GHGs.loc[:, 'GWh_TOTAL'] * 3.6
    
        GHGs.dropna(axis=1, how='all', inplace=True)
    
        return GHGs
        
    
    def energy_merge(ghgrp_energy, all_fac, df_AA_energy):
        
        merge_cols = list(all_fac.columns.difference(df_AA_energy.columns))
    
        merge_cols.append('FACILITY_ID')
    
        df_AA_energy = pd.merge(
            df_AA_energy, all_fac[merge_cols], how='inner', on='FACILITY_ID'
            )
        
        ghgrp_energy = pd.concat([ghgrp_energy, df_AA_energy], ignore_index=True)
        
        return ghgrp_energy
    
    
    def id_industry_groups(GHGs):
        """
        Assign industry groupings based on NAICS codes 
        """
        grouping_dict = {
            'Agriculture': [111,115], 'Mining and Extraction': [211, 212, 213], 
            'Utilities': 221, 'Construction': [236, 237, 238], 
            'Food Manufacturing': 311, 'Beverage Manufacturing':312, 
            'Textile Mills': 313, 'Textile Product Mills': 314,
            'Wood Product Manufacturing': 321, 'Paper Manufacturing': 322,
            'Printing': 323, 'Petroleum and Coal Products': 324,
            'Chemical Manufacturing': 325, 'Plastics and Rubber Products': 326,
            'Nonmetallic Mineral Products': 327, 'Primary Metals': 331,
            'All other Manufacturing': [332, 333, 334, 335, 336, 337, 338, 339],
            'Wholesale Trade': [423, 424,425],
            'Retail Trade': [441, 442, 443, 444, 445, 446, 447, 448, 451, 452,453,
             454], 'Transportation and Warehousing': [481, 482, 483,484, 485, 
             486, 487, 488,491, 492, 493], 'Publishing': [511, 512, 515, 517, 518,
             519],'Finance and Insurance': [521, 522, 523, 524, 525], 
            'Real Estate and Leasing': [531, 532, 533], 
            'Professional, Scientific, and Technical Services': 541, 
            'Management of Companies': 551, 
            'Admini and Support and Waste Management and Remediation Services': 
            [561, 562], 'Educational Services': 611, 
            'Health Care and Social Assistance': [621, 622, 623, 624], 
            'Arts and Entertainment': [711, 712, 713], 
            'Accommodation and Food Services': [721, 722],'Other Services': [811, 
            812, 813, 814], 'Public Adminsitration': [921, 
            922, 923, 924, 925, 926, 927, 928]
            }
    
        gd_v = []
    
        gd_k = []
    
        for v in list(grouping_dict.values()):
    
            kname = [k for k in grouping_dict if grouping_dict[k] == v][0]
    
            if type(v) == int:
    
                gd_v.append(v)
    
                gd_k.append(kname)
    
            else:
    
                gd_v.extend(w for w in v)
    
                gd_k.extend(kname for w in v)
    
        gd_df = pd.DataFrame(columns=('PNC_3', 'GROUPING'))
    
        gd_df.PNC_3 = gd_v
    
        gd_df.GROUPING = gd_k
    
        gd_df.set_index('PNC_3', inplace = True)
    
        GHGs['PNC_3'] = \
            GHGs['PRIMARY_NAICS_CODE'].apply(lambda n: int(str(n)[0:3]))
    
        GHGs = pd.merge(GHGs, gd_df, left_on = GHGs.PNC_3, right_index=True)
    
        #Identify manufacturing groupings
        gd_df['MFG'] = \
            gd_df.GROUPING.apply(
                lambda x: x in [gd_df.loc[i, :].values[0] for i in gd_df.index if
                str(i)[0] == '3']
                )
    
        gd_df['IND'] = gd_df.GROUPING.apply(
    	    lambda x: x in [gd_df.loc[i,:].values[0] for i in gd_df.index if 
    		   ((str(i)[0] == '3') | (str(i)[0] == '1') | ((str(i)[0] == '2') & \
                (str(i) != '221')))]
                )
    
        GHGs_E_dict = {}
    
        GHGs_GHG_dict = {}
    
        g_names = ['Ind_' + str(GHGs.REPORTING_YEAR.drop_duplicates().values[0])]
    
        for g_n in g_names:
    
            GHGs_GHG_dict[g_n] = \
                pd.DataFrame(
                    GHGs[(GHGs['REPORTING_YEAR'] == int(g_n[-4:])) & 
                        (GHGs['CO2e_TOTAL'] > 0)]
                    )
    
            GHGs_E_dict[g_n] = \
                pd.DataFrame(
                    GHGs[(GHGs['REPORTING_YEAR']==int(g_n[-4:])) & 
                        (GHGs['MMBtu_TOTAL'] > 0)]
                    )
                    
        return GHGs

#
#GHGs = format_GHGRP_emissions(c_fuel_file, d_fuel_file)
#
#facdata = format_GHGRP_facilities(facilities_file)
#
#GHGs = calculate_energy(GHGs, facdata, EFs)
#
#GHGs = id_industry_groups(GHGs)
#
#GHGs.to_csv('ghgrp_energy_' + str(year) + '.csv')
