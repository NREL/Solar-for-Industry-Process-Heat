# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 09:42:59 2019

@author: cmcmilla
"""
import pandas as pd
import os

class end_use:
    
    def __init__(self):
        
        self.datadir = '../calculation_data'
        
        self.ihs_file = 
    
    def import_IHS_data(ihs_file):
        """
        Imports heat characteristic data by NAICS and end use.
        """
        ihs_data = pd.read_excel(ihs_file, sheetname=['MECS', 'HeatChar'])
    
        ihs_data['MECS'].dropna(axis=0, inplace=True)
    
        ihs_data['MECS'].loc[:, 'END_USE'] = ihs_data['MECS'].END_USE.str.strip()
    
        ihs_data['MECS'].loc[:, 'NAICS_CODE'] = \
            ihs_data['MECS'].NAICS_CODE.astype(int)
    
        ihs_data['MECS'].set_index(['NAICS_CODE', 'END_USE'], inplace=True)
    
        return ihs_data
    
    
    def enduse_calc(target_baseline, ihs_data, eu_file):
        """
        Calculates end use fraction of target industry combustion fuel use (in TJ).
        All industries except Potash, Soda, and Borate Mining based on 2010 
        MECS data. All combustion energy of Potash Mining assumed to meet
        300 deg C demands, either in rotarty gas-fired calciners or steam tube
        dryers. It's possible to separate out boiler energy into end uses by
        industry based on data in Steam_end_uses.csv.
        """
    
        eu_dict = dict(pd.read_csv(eu_file, encoding='latin1').values)
        
        hterms = ['furnace', 'kiln', 'dryer', 'heater', 'oven']
      
        enduses = list(
            ihs_data['MECS'].index.get_level_values(1).drop_duplicates()
            )
    
        target_enduse = pd.DataFrame(columns=[
                            'REPORTING_YEAR', 'COUNTY_FIPS',
                            'FINAL_NAICS_CODE', 'STATE', 'CITY', 'COUNTY',
                            'FACILITY_ID', 'FUEL_TYPE', 'FUEL_TYPE_OTHER',
                            'FUEL_TYPE_BLEND', 'END_USE'
                            ])
    
        target_baseline.set_index(
            ['REPORTING_YEAR', 'COUNTY_FIPS', 'FINAL_NAICS_CODE'],
            inplace=True, drop=False
            )
    
        # Create dataframe to store amount of end use energy not calculated from
        # MECS data.
        eu_noMECS = pd.DataFrame()
    
        # Calculate energy by end use
        for n in target_baseline[
            target_baseline.MECS_NAICS.notnull()
            ].MECS_NAICS.drop_duplicates():
    
            for f in target_baseline.MECS_FT.dropna().drop_duplicates():
    
                # Map specified UNIT_TYPE to MECS end uses. Not done for most
                # GHGRP UNIT_TYPES due to limited accompanying detail.
                FT_enduse = target_baseline[
                        (target_baseline.MECS_NAICS == n) &
                        (target_baseline.MECS_FT == f)
                        ].UNIT_TYPE.map(eu_dict)
    
                FT_enduse.name = 'END_USE'
    
                FT_enduse = pd.concat(
                        [FT_enduse, target_baseline[
                            (target_baseline.MECS_NAICS == n) & 
                            (target_baseline.MECS_FT == f)
                            ][['STATE', 'COUNTY', 'CITY', 'FACILITY_ID',
                                'UNIT_NAME', 'UNIT_TYPE', 'FUEL_TYPE',
                                'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND', 'TJ'
                                ]]
                            ], axis=1
                        )
    
                # Check for process heating-related terms in UNIT_NAME if UNIT_TYPE
                # == 'OCS (Other combustion source).
                try:
                    FT_enduse[FT_enduse.UNIT_TYPE == 'OCS (Other combustion source)'
                        ].values[0]
    
                except IndexError:
                    pass
      
                else:
                    other_heat = pd.DataFrame(FT_enduse[
                        FT_enduse.UNIT_TYPE == 'OCS (Other combustion source)'
                        ], copy=True)
        
                    other_heat.rename(
                        columns={'UNIT_NAME': 'UNIT_NAME_og'}, inplace=True
                        )
    
                    for t in hterms:
                        other_heat = pd.concat([other_heat, FT_enduse[
                            FT_enduse.UNIT_TYPE == 'OCS (Other combustion source)'
                            ].UNIT_NAME.apply(lambda x: t in str.lower(x))], axis=1)
    
                    other_heat.loc[:, 'PH'] = \
                        other_heat.iloc[:, (-4):].sum(axis=1).map(
                            {1: 'Process Heating'}
                            )
    
                    other_heat.drop('UNIT_NAME', axis=1, inplace=True)
                
                    for k, v in dict(
                        other_heat[other_heat.PH.notnull()]
                            [['FACILITY_ID', 'UNIT_NAME_og']].values).items():
                                FT_enduse.loc[(FT_enduse.FACILITY_ID == k) & 
                                (FT_enduse.UNIT_NAME == v), 'END_USE'] = \
                                    'Process Heating'
    
                FT_enduse.reset_index(drop=False, inplace=True)
    
                eu_noMECS = pd.concat(
                    [eu_noMECS, pd.DataFrame(
                        {'FINAL_NAICS_CODE': n, 'MECS_FT': f,
                        'TJ': FT_enduse[FT_enduse.END_USE.notnull()].TJ.sum()
                        }, index=[0]
                        )], ignore_index=True, axis=0
                    )
    
                # FT_enduse.loc[FT_enduse.END_USE.isnull(), 'TJ'] = np.nan
                
    #            FT_enduse.reset_index(inplace=True, drop=False)
    #            
    #            FT_enduse.set_index(['REPORTING_YEAR', 'FACILITY_ID'], drop=True,
    #                                inplace=True)
    #            
    #            fac_TJ_FT = pd.DataFrame(
    #                target_baseline[target_baseline.MECS_FT == f].groupby(
    #                    ['REPORTING_YEAR', 'FACILITY_ID']
    #                    ).TJ.sum()
    #                )
    #            
    #            fac_TJ_FT.rename(columns={'TJ': 'TJ_FT'}, inplace=True)
    #            
    #            FT_enduse = FT_enduse.merge(fac_TJ_FT, how='inner',
    #                                        left_index=True, right_index=True)
    
                # Calculate the defined end use fraction of total energy use
                # by fuel type.
    #            FT_enduse.loc[:, 'EUFT_fraction'] = np.nan
    #            
    #            FT_enduse.loc[:, 'EUFT_fraction'] = \
    #                FT_enduse.TJ.divide(FT_enduse.TJ_FT,fill_value=0)
    #
    #            for c in ['END_USE', 'EUFT_fraction']:            
    #                FT_enduse.loc[FT_enduse[c].isnull(), 'EUFT_fraction'] = 0
    
                # Calculate end use energy for remaining unit types.
                if ihs_data['MECS'].ix[n][f].sum() > 0:
                   
                    # correction factor for energy use already assigned to an
                    # end use.
                  
    #                FT_enduse.reset_index(drop=False, inplace=True)
    #
    #                ft_corr = 1 - FT_enduse.groupby(
    #                    ['REPORTING_YEAR', 'FACILITY_ID', 'END_USE']
    #                    ).EUFT_fraction.sum()
    
                    fac_eu = FT_enduse[FT_enduse.END_USE.isnull()].TJ.apply(
                        lambda x: x * ihs_data['MECS'].ix[n][f]
                        )
    
                    fac_eu = pd.concat(
                        [FT_enduse[FT_enduse.END_USE.isnull()], fac_eu], axis=1
                        )
    
                    fac_eu.drop(['END_USE', 'TJ'], axis=1, inplace=True)
                    # FT_enduse.reset_index(inplace=True)
    
                    fac_eu = pd.melt(
                        # FT_enduse,
                        fac_eu,
                        id_vars=[
                            'REPORTING_YEAR', 'COUNTY_FIPS', 
                            'FINAL_NAICS_CODE', 'STATE', 'CITY', 'COUNTY',
                            'FACILITY_ID', 'FUEL_TYPE', 'FUEL_TYPE_OTHER',
                            'FUEL_TYPE_BLEND', 'UNIT_NAME', 'UNIT_TYPE'
                            ], value_name=f, var_name='END_USE'
                        )
    
                    FT_enduse.rename(columns={'TJ': f}, inplace=True)
                    
                    target_enduse = target_enduse.append(
                        pd.concat([fac_eu, FT_enduse[FT_enduse.END_USE.notnull()]],
                        ignore_index=True)
                        )
    
                else:
                    pass
    
        for c in ['REPORTING_YEAR', 'COUNTY_FIPS', 'FINAL_NAICS_CODE']:
            target_enduse.loc[:, c] = target_enduse[c].apply(lambda x: int(x))
    
        target_enduse.loc[:, 'MECS_NAICS'] = \
            target_enduse.loc[:, 'FINAL_NAICS_CODE'].map(dict(
               target_baseline[['FINAL_NAICS_CODE', 'MECS_NAICS']].values
               )
            ).apply(lambda x: int(x))
    
        target_enduse.dropna(how='all', inplace=True, axis=1)
    
        target_enduse.fillna(0, inplace=True)
    
        #target_enduse = target_enduse.groupby(
        #    ['REPORTING_YEAR', 'COUNTY_FIPS', 'FINAL_NAICS_CODE', 'END_USE'], 
        #    as_index=True
        #    ).sum()
    
        target_enduse.loc[:, 'Total'] = target_enduse[['Coal', 'Diesel', 'LPG_NGL',
            'Natural_gas', 'Other', 'Residual_fuel_oil']].sum(axis=1)
    
        target_enduse.reset_index(drop=True, inplace=True)
    
        return {'target_enduse': target_enduse, 'eu_noMECS': eu_noMECS}
    
    def heat_mapping(target_enduse, ihs_data, char=None):
        """
        Map heat use characteristics (e.g., temperature) to end use
        disggregation. Fuel use from mfg_end
        """
    
        char_out = pd.DataFrame()
    
        if char == 'temp':
    
            char_out.loc[:, 'Temp_degC'] = []
    
            char_out.loc[:, 'Alt_supply'] = []   
    
            for g in ihs_data['HeatChar'].groupby(['NAICS', 'End_use']).groups:
    
                if len(ihs_data['HeatChar'].groupby(
                            ['NAICS', 'End_use']
                            ).get_group(g).index) == 1:
             
                    char_out = char_out.append(
                            target_enduse.groupby(
                            ['FINAL_NAICS_CODE', 'END_USE']
                                ).get_group(g), ignore_index=True)
    
                    char_index = char_out.groupby(
                        ['FINAL_NAICS_CODE', 'END_USE']
                        ).get_group(g).index
    
                    char_out.loc[char_index, 'Temp_degC'] = \
                            ihs_data['HeatChar'].groupby(
                                ['NAICS', 'End_use']
                                ).get_group(g)['Temp_degC'].values[0]
                                
                    char_out.loc[char_index, 'Alt_supply'] = \
                            ihs_data['HeatChar'].groupby(
                                ['NAICS', 'End_use']
                                ).get_group(g)['Alt_supply'].values[0]
    
                else:
                    for i in ihs_data['HeatChar'].groupby(
                                ['NAICS', 'End_use']
                                ).get_group(g).index:
    
                        e_temp = pd.DataFrame(target_enduse.groupby(
                            ['FINAL_NAICS_CODE', 'END_USE']
                            ).get_group(g), copy=False)
    
                        e_temp.loc[:, 
                            ('LPG_NGL', 'Natural_gas', 'Other', 'Residual_fuel_oil',
                             'Total')] = \
                                e_temp.loc[:, ('LPG_NGL', 'Natural_gas', 'Other',
                                'Residual_fuel_oil', 'Total')] * \
                                    ihs_data['HeatChar'].loc[i, 'Fraction']
    
                        e_temp.loc[:, 'Temp_degC'] = \
                            ihs_data['HeatChar'].loc[i, 'Temp_degC']
                            
                        
                        e_temp.loc[:, 'Alt_supply'] = \
                            ihs_data['HeatChar'].loc[i, 'Alt_supply']
    
                        char_out = pd.concat([char_out, e_temp], ignore_index=True)
    
        char_out.Alt_supply.fillna(value=False, inplace=True)
    
        char_out.reset_index(drop=True, inplace=True)
    
        return char_out
    
       
    def ghg_calc(efs_file, char_out, fuelxwalkDict):
        """
        Calculates CO2e for target industries and identified end uses.
        """
        efs = pd.read_csv(efs_file)
    
        efs.drop_duplicates(subset='Fuel_Type', inplace=True)
    
        efs.set_index('Fuel_Type', drop=True, inplace=True)
    
        for c in ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']:
    
            for ft in char_out[c].drop_duplicates():
                if ft in efs.index:
    
                    char_index = char_out[char_out[c] == ft].index
     
                    char_out.loc[char_index, 'MMTCO2E'] = \
                        char_out.loc[char_index, fuelxwalkDict[ft]] * 947.817 * \
                        (efs.loc[ft, 'CO2_kgCO2_per_mmBtu'] +
                         efs.loc[ft, 'CH4_gCH4_per_mmBtu'] / 1000 * 25 + 
                         efs.loc[ft, 'N2O_gN2O_per_mmBtu'] / 1000 * 298
                         ) / 1000000000
    
                else:
                    pass
    
        for c in ['COUNTY_FIPS', 'FACILITY_ID', 'REPORTING_YEAR', 'MECS_NAICS',
                  'FINAL_NAICS_CODE']:
    
            char_out.loc[:, c] = [int(x) for x in char_out[c]]
    
        return char_out