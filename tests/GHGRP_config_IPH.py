# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 14:05:05 2017

@author: cmcmilla
"""

import pandas as pd
import datetime
import os
import Get_GHGRP_data
import GHGRP_energy_calc
import GHGRP_AAenergy_calc

class GHGRP_calcs:
    
    def __init__(self, years):
        
        self.years = range(years[0], years[0])

        self.table_dict = {'subpartC': 'C_FUEL_LEVEL_INFORMATION',
                      'subpartD': 'D_FUEL_LEVEL_INFORMATION',
                      'subpartV_fac': 'V_GHG_EMITTER_FACILITIES', 
                      'subpartV_emis': 'V_GHG_EMITTER_SUBPART',
                      'subpartAA_ff':'AA_FOSSIL_FUEL_INFORMATION', 
                      'subpartAA_liq': 'AA_SPENT_LIQUOR_INFORMATION'}
        
        # Set GHGRP data file directory
        self.ghgrp_file_dir = \
            os.path.join('./', 'calculation_data/ghgrp_data/')

        # List of facilities for correction of combustion emissions from Wood 
        #and Wood Residuals for using Subpart C Tier 4 calculation methodology.
        self.wood_facID = pd.read_csv(os.path.join('./', 
                'calculation_data/WoodRes_correction_facilities.csv'
                ),index_col=['FACILITY_ID'])

# Data on CO2 and CH4 emission factors by fuel type 
#EFs = pd.read_csv(" +
#    "/EPA_FuelEFs.csv", index_col=['Fuel_Type']
#    )

EFs['dup_index'] = EFs.index.duplicated()

EFs = pd.DataFrame(EFs[EFs.dup_index == False], columns=EFs.columns[0:2])




ghgrp_data = {}

ghgrp_y = pd.DataFrame()

ghgrp_energy = pd.DataFrame()

#aa_sl_table = pd.DataFrame()
#
#aa_ffuel_table = pd.DataFrame()

    # Get data from EPA API if not available
    def ghgrp_dl_save(self, subpart, year):
        
        if subpart in ['subpartC', 'subpartD']:
        
            filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
        
            if filename not in os.listdir(file_dir):
        
                data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                
                data_y.to_csv(file_dir  + filename + '.csv')
                
            else:
    
                data_y = pd.read_csv(file_dir + filename)
                
        if subpart == 'subpartV_fac':
            
            filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
        
            if filename not in os.listdir(file_dir):
        
                data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                
                data_y.to_csv(file_dir  + filename + '.csv')
                
            else:
    
                data_y = pd.read_csv(file_dir + filename)
    
        if subpart == 'subpartV_emis':
                        filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
        
            if filename not in os.listdir(file_dir):
        
                data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                
                data_y.to_csv(file_dir  + filename + '.csv')
                
            else:
    
                data_y = pd.read_csv(file_dir + filename)
            
        if subpart == 'subpartAA_ff':
                        filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
        
            if filename not in os.listdir(file_dir):
        
                data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                
                data_y.to_csv(file_dir  + filename + '.csv')
                
            else:
    
                data_y = pd.read_csv(file_dir + filename)
            
        if subpart == 'subpartAA_liq':
                        filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
        
            if filename not in os.listdir(file_dir):
        
                data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                
                data_y.to_csv(file_dir  + filename + '.csv')
                
            else:
    
                data_y = pd.read_csv(file_dir + filename)
        
    for subpart, table in self.table_dict.items():
    
        for y in years:
            
            if subpart in ['subpartC', 'subpartD']:
            
                filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
            
                if filename not in os.listdir(file_dir):
            
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                    
                    data_y.to_csv(file_dir  + filename + '.csv')
                    
                else:
    
                    data_y = pd.read_csv(file_dir + filename)
                    
            if subpart == 'subpartV_fac':
                
                filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
            
                if filename not in os.listdir(file_dir):
            
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                    
                    data_y.to_csv(file_dir  + filename + '.csv')
                    
                else:
    
                    data_y = pd.read_csv(file_dir + filename)
    
            if subpart == 'subpartV_emis':
                            filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
            
                if filename not in os.listdir(file_dir):
            
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                    
                    data_y.to_csv(file_dir  + filename + '.csv')
                    
                else:
    
                    data_y = pd.read_csv(file_dir + filename)
                
            if subpart == 'subpartAA_ff':
                            filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
            
                if filename not in os.listdir(file_dir):
            
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                    
                    data_y.to_csv(file_dir  + filename + '.csv')
                    
                else:
    
                    data_y = pd.read_csv(file_dir + filename)
                
            if subpart == 'subpartAA_liq':
                            filename = self.table_dict[subpart][0:7].lower() + str(y) + '.csv'
            
                if filename not in os.listdir(file_dir):
            
                    data_y = Get_GHGRP_data.get_GHGRP_records(y, table)
                    
                    data_y.to_csv(file_dir  + filename + '.csv')
                    
                else:
    
                    data_y = pd.read_csv(file_dir + filename)
                
                
    
            ghgrp_data[subpart] = ghgrp_data[subpart].append(data_y,
                      ignore_index=True)
    
        
    #for y in years:
    #          
    #   c_fuel_table = Get_GHGRP_data.get_GHGRP_records(y, tables[0])
    #
    #   c_fuel_table.to_csv('c_fuel_' + str(y) + '.csv')
    #
    #   d_fuel_table = Get_GHGRP_data.get_GHGRP_records(y, tables[1])
    #
    #   d_fuel_table.to_csv('d_fuel_' + str(y) + '.csv')
    #
    #   fac_table = Get_GHGRP_data.get_GHGRP_records(y, tables[2])
    #
    #   fac_table.to_csv('fac_table_' + str(y) + '.csv')
    #
    #   for t in [c_fuel_table, d_fuel_table, fac_table]:
    #       t.to_csv(str(t) + '_' + str(y) + '.csv')
    
    # Subpart AA tables are much smaller and addressed slightly differently
    #for y in years:
    #    
    #    aa_sl_table = aa_sl_table.append(
    #        Get_GHGRP_data.get_GHGRP_records(y, tables[5])
    #        )
    #    aa_ffuel_table = aa_ffuel_table.append(
    #        Get_GHGRP_data.get_GHGRP_records(y, tables[4])
    #        )
    #
    #aa_sl_file = "aa_sl_table_" + str(years[0]) + str(years[-1]) + ".csv"
    #    
    #aa_sl_table.to_csv(file_dir + aa_sl_file)
    #
    #aa_ffuel_file = "aa_ffuel_table_" + str(years[0]) + str(years[-1]) + ".csv"
    #
    #aa_ffuel_table.to_csv(file_dir + aa_ffuel_file)
    
    
    facfile_2010 = file_dir + "fac_table_2010.csv"
    
    #facfiles_201115 = []
    
    for y in years:
        
        facfiles_201115.append(
            file_dir + "fac_table_" + str(y) + ".csv"
            )
    
    # Finding missing FIPS codes in format_GHGRP_facilities takes a long time due 
    # to the way a API query is currently written. 
    facdata = GHGRP_energy_calc.format_GHGRP_facilities
    facdata = GHGRP_energy_calc.format_GHGRP_facilities(
        facfile_2010, facfiles_201115
        )
    
    for y in years:
        GHGs_y = \
            GHGRP_energy_calc.format_GHGRP_emissions(c_file, d_file)
    
        GHGs_y = GHGRP_energy_calc.calculate_energy(
            GHGs_y, facdata, EFs, wood_facID
            )
    
        ghgrp_energy = ghgrp_energy.append(GHGs_y)
    #    c_file = file_dir + "c_fuel_" + str(y) + ".csv"
    #    d_file = file_dir + "d_fuel_" + str(y) + ".csv"
    
        GHGs_y = \
            GHGRP_energy_calc.format_GHGRP_emissions(c_file, d_file)
    
        GHGs_y = GHGRP_energy_calc.calculate_energy(
            GHGs_y, facdata, EFs, wood_facID
            )
    
        ghgrp_energy = ghgrp_energy.append(GHGs_y)
    
    # Calculate energy for Subpart AA reporters
    GHGs_FF = GHGRP_AAenergy_calc.format_GHGRP_AAff_emissions(
        file_dir + aa_ffuel_file
        )
    
    GHGs_SL = GHGRP_AAenergy_calc.format_GHGRP_AAsl_emissions(
        file_dir + aa_sl_file
        )
    
    AA_FF_energy = GHGRP_AAenergy_calc.MMBTU_calc_AAff(GHGs_FF, EFs)
    
    AA_SL_energy = GHGRP_AAenergy_calc.MMBTU_calc_AAsl(GHGs_SL)
    
    AA_energy = GHGRP_AAenergy_calc.AA_merge(AA_FF_energy, AA_SL_energy)
    
    # Merge calculated energy values for Subparts AA, C, and D
    ghgrp_energy = GHGRP_energy_calc.energy_merge(ghgrp_energy, facdata, AA_energy)
    
    ghgrp_energy = GHGRP_energy_calc.id_industry_groups(ghgrp_energy)
    
    # Add timestamp to output csv file.
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    
    outputfile = 'GHGRP_all_' + ts + '.csv'
    
    ghgrp_energy[['CITY', 'COUNTY', 'COUNTY_FIPS', 'ZIP',
        'FACILITY_ID', 'FACILITY_NAME', 'FUEL_TYPE', 'FUEL_TYPE_BLEND',
        'FUEL_TYPE_OTHER', 'GROUPING', 'MECS_Region', 'PARENT_COMPANY', 'PNC_3',
        'PRIMARY_NAICS_CODE', 'SECONDARY_NAICS_CODE', 'ADDITIONAL_NAICS_CODES',
        'REPORTING_YEAR', 'STATE', 'STATE_NAME', 'UNIT_NAME', 'UNIT_TYPE',
        'MMBtu_TOTAL']].to_csv(file_dir + outputfile)
