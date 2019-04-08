# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 21:50:55 2018

@author: cmcmilla
"""

import pandas as pd
import os
import numpy as np
from Get_GHGRP_data_IPH import get_GHGRP_records

#%%
class FuelUncertainty:
    """
    Description of Subpart C tables reporting heat content or carbon content
    of fuels. From https://www.epa.gov/enviro/greenhouse-gas-customized-search
    
    C_T2EQC2AMONTHLYINPUTS: A collection of data elements containing the 
    monthly measured High Heat Value (HHV) or, if applicable, an appropriate 
    substitute data value, and the monthly mass or volume combusted, for a
    fuel using the Tier 2 calculation methodology, except for Municipal Solid
    Waste (MSW).
    (Collected for 2014 onwards)
        
    C_T2EQC2CMONTHLYINPUTS:A collection of data elements containing the total
    monthly mass of steam generated, and the monthly measured ratio of the
    boiler's maximum rated heat input capacity to its design rated steam 
    output capacity, for Municipal Solid Waste (MSW) or any other solid fuel
    using the Tier 2 calculation methodology. (Collected for 2014 onwards)
     
    C_T3EQC3C8MONTHLYINPUTS: A collection of data elements containing the
    monthly mass combusted and the monthly average carbon content for a solid
    fuel using the Tier 3 calculation methodology. (Collected for 2014 onwards)
    
    C_T3EQC4C8MONTHLYINPUTS: A collection of data elements containing the
    monthly volume combusted and the monthly average carbon content for a
    liquid fuel using the Tier 3 calculation methodology.
    (Collected for 2014 onwards) 
    
    C_T3EQC5C8MONTHLYINPUTS: A collection of data elements containing the
    monthly  volume combusted, the monthly average carbon content, and the
    monthly average molecular weight for a gaseous fuel using the Tier 3
    calculation methodology.
    (Collected for 2014 onwards)
    
    ------
    Aggregations (agr)
    ------
    'by_fuel': default
    'by_fuel_year': annual fuel 
    'by_facility_year': annual fuel by facility
    
    """

    def __init__(self, agr=None, years=None, std_efs=None):
        
        self.tier_tables = {'t2_hhv': ['C_T2EQC2AMONTHLYINPUTS'],
                            't2_boiler': ['C_T2EQC2CMONTHLYINPUTS'], 
                            't3': ['C_T3EQC3C8MONTHLYINPUTS',
                                   'C_T3EQC4C8MONTHLYINPUTS', 
                                   'C_T3EQC5C8MONTHLYINPUTS']}
        
        self.ef_file_path = os.path.join('../', 'calculation_data/',
                                         'EPA_FuelEFs.csv')
        
        if std_efs is None:
        
            self.fuel_efs = pd.read_csv(self.ef_file_path)
            
        else:
            
            self.fuel_efs = std_efs
            
        self.fuel_efs.columns = [x.lower() for x in self.fuel_efs.columns]
        
        if agr == None:
            
            self.agr = 'by_fuel'
        
        self.years = [years]

    def dl_tier(self, tier):
        """
        Downloads and formats EPA GHGRP-reported data on heat
        rates, carbon content, and related data for facilities reporting
        GHGs under Subart C (stationary combustion) using Tier 2 or Tier 3 
        approaches.
        
        ---------------
        Available tiers
        ---------------
        t2_hhv: HHV values by fuel for Tier 2 methodology
        t2_boiler: Boiler steam production data for Tier 2
        t3: Carbon content by fuel for Tier 3 methodology
        
        """
        
        tier_df = pd.DataFrame()

        for t in self.tier_tables[tier]:

            for y in self.years:
                
                df = get_GHGRP_records(y, t)
                
                tier_df = tier_df.append(df, sort=True, ignore_index=True)

        tier_df.columns = [x.lower() for x in tier_df.columns]
        
        # Fix issues with natural gas HHV reporting
        # Other fuel HHVs were exammined manually. There's a wide range for
        # wood and wood residuals, but not other fuels.
        if tier == 't2_hhv':
            
            tier_df['high_heat_value'] = \
                tier_df.high_heat_value.astype('float32')
        
            natgas_st_index = tier_df[
                (tier_df.fuel_type == 'Natural Gas (Weighted U.S. Average)') & 
                (tier_df.high_heat_value_uom == 'mmBtu/short ton')
                ].index
            
            tier_df.loc[natgas_st_index, 'high_heat_value_uom'] = 'mmBtu/scf'
            
            m_index = tier_df[
                (tier_df.fuel_type == 'Natural Gas (Weighted U.S. Average)') & 
                (tier_df.high_heat_value.between(1, 1.2))
                ].index
            
            tier_df.high_heat_value.update(
                    tier_df.loc[m_index, 'high_heat_value'].divide(1000)
                    )
            
            drop_index = tier_df[
                (tier_df.fuel_type == 'Natural Gas (Weighted U.S. Average)') & 
                (tier_df.high_heat_value.between(0.0012, 0.0014))
                ].index
            
            tier_df = tier_df[~tier_df.index.isin(drop_index)]
            
        return tier_df
        
    def summarize_tier(self, tier_df):
        """
        Calculates a statistical summary (mean, min, max, std) of
        a data field for a Subpart C calculation approach Tier.
    
        """
        
        def calc_summary(tier_df, group_key, c):
            
            group_keys_dict = {
                'by_facility_year': ['facility_id', 'reporting_year',
                                     'fuel_type'],
                'by_fuel_year':['reporting_year', 'fuel_type'],
                'by_fuel': ['fuel_type']
                }
                
            if c in tier_df.columns:
                    
                if c == 'bioler_ratio_b':
                    
                    column_unit = 'boiler_heat_uom'

                else:
                    
                    column_unit = c + '_uom'

                tier_df[c] = tier_df[c].astype('float32')

                group_keys_dict[group_key].append(column_unit)
                
                result = \
                    tier_df[tier_df[c] > 0].groupby(
                        group_keys_dict[group_key])[c].agg(
                             ['count', 'mean', 'min', 'max', 'std']
                             )

                result = pd.merge(
                    result.reset_index(), 
                    self.fuel_efs[['fuel_type', 'co2_kgco2_per_mmbtu']],
                    on='fuel_type', how='outer'
                    )
                
                result.set_index(group_keys_dict[group_key],
                                               inplace=True)
                
                result = result.dropna(subset=['mean'])
                        
                return result
        
        results_dict = {}
    
        for key in ['by_fuel_year', 'by_facility_year', 'by_fuel']:
                
            results_dict[key] = {}
        
            for c in ['fuel_combusted', 'molecular_weight', 'carbon_content',
                      'high_heat_value', 'mass_of_steam', 'boiler_ratio_b']:
                
                results_dict[key][c] = calc_summary(tier_df, key, c)

        return results_dict
    
    def calc_error_prop(self, tier2_hhv, tier3):
        """
        Calculate error propogation of carbon content of fuels
        (kg CO2/mmBtu)--agregated by fuel, reporting year and fuel, or
        reporting year, fuel, and facility--based on data reported for Tier 2 
        and Tier 3 emission calculation approaches.
    
        """
        
        def calc_sq_std(df):
            """
            Calculate the square of the standard deviation divided by 
            mean. Returns a dataframe.
            """
            
            sq_std = df.dropna()

            sq_std = (df['std'].divide(df['mean']))**2
            
            sq_std.name = 'sq_std'
            
            sq_std = pd.DataFrame(sq_std)
            
            sq_std = sq_std.dropna()
            
            return sq_std
        
        gas_scf_to_kg = pd.DataFrame()
        
        for f in ['Natural Gas (Weighted U.S. Average)', 'Fuel Gas']:
            
            scf_df = tier3[self.agr]['molecular_weight'].xs(
                        f, level='fuel_type'
                        )
            
            scf_df['fuel_type'] = f
            
            gas_scf_to_kg = gas_scf_to_kg.append(scf_df)
            
        gas_scf_to_kg.set_index('fuel_type', append=True, inplace=True)
        
        gas_scf_to_kg.reset_index('molecular_weight_uom', inplace=True,
                                  drop=False)
        
        error_prop = pd.merge(
            calc_sq_std(
                tier2_hhv[self.agr]['high_heat_value']
                ).reset_index(level=1),
            calc_sq_std(
                tier3[self.agr]['carbon_content']
                ).reset_index(level=1),
            left_index=True, right_index=True, how='inner',
            suffixes=['_hhv', '_C'])
            
        # Include error of molecular weight of fuel gas and natural gas
        error_prop = pd.merge(error_prop, calc_sq_std(gas_scf_to_kg), 
                              left_index=True, right_index=True, how='outer')
        
        error_prop.rename(columns={'sq_std': 'sq_std_mm'}, inplace=True)
        
        error_prop['final_uncert'] = error_prop.sq_std_hhv.add(
                error_prop.sq_std_C
                ).add(
                    error_prop.sq_std_mm, fill_value=0
                    )
        
        return error_prop, gas_scf_to_kg
    
    
    def calc_EF(self, error_prop, tier2_hhv_summary, tier3_summary,
                gas_scf_to_kg):
        """
        Calculate CO2 emission factors by fuel type (kg CO2/mmBtu) based on 
        HHV values from Tier 2 tables, and carbon content and molecular
        mass (where appropriate) from Tier 3 tables.
        """
        
        t2t3_efs = pd.DataFrame()
        
        uom_dict = {'percent by weight, expressed as a decimal fraction': \
                        'mmBtu/short ton',
                    'kg C per kg': 'mmBtu/scf',
                    'kg C per gallon': 'mmBtu/gallon'}
        
        # Calculate kg-mol per SCF for natural gas and fuel gas. SCF defined in
        # SI units as 101.560 kPa, 288.706 K, 0.02316847 m3. Ideal gas
        # constant is 8.314 kPa*m3/(kg-mol*K)
        scf_per_kgmol = (8.314 * 288.706) / (101.560 * 0.028316847)
    
        conv_dict = {'percent by weight, expressed as a decimal fraction': \
                     907.185, 'kg C per kg': scf_per_kgmol,
                     'kg C per gallon': 1}
        
        for uom in uom_dict.keys():

            if uom == 'kg C per kg':
                
                 t2t3_efs = t2t3_efs.append(pd.DataFrame(
                    tier2_hhv_summary[self.agr]['high_heat_value'].xs(
                        uom_dict[uom], level=1
                        )['mean'].multiply(conv_dict[uom]).divide(
                            gas_scf_to_kg['mean']
                            ).divide(
                                tier3_summary[self.agr]['carbon_content'].xs(
                                    uom, level=1
                                    )['mean']
                                ) * (12/44)
                    )**-1)

            else:
                
                t2t3_efs = t2t3_efs.append(pd.DataFrame(
                    tier3_summary[self.agr]['carbon_content'].xs(
                        uom, level=1
                    )['mean'].divide(
                        tier2_hhv_summary[self.agr]['high_heat_value'].xs(
                            uom_dict[uom], level=1
                            )['mean']
                        ) * (44/12) * conv_dict[uom]
                    ))

        t2t3_efs = pd.merge(t2t3_efs, error_prop[['final_uncert']],
                            left_index=True, right_index=True,
                            how='inner')
        
        t2t3_efs.rename(columns={'mean': 'kgCO2_per_mmBtu'}, inplace=True)
        
        # Create column for the uncertainty amount in kg CO2/mmBtu (+/-)
        t2t3_efs['ef_plus_minus'] = t2t3_efs.kgCO2_per_mmBtu.multiply(
                t2t3_efs.final_uncert
                )
        
        return t2t3_efs
    
    def create_efs(self, t2t3_efs):
        """
        Make a dataframe with columns of standard EPA emission factors and 
        substitutes from uncertainty calculations. 
        """
        
        efs_for_energy = pd.DataFrame(
            np.tile(self.fuel_efs.co2_kgco2_per_mmbtu.values, (4, 1)).T,
            columns=['standard', 'calculated', 'uncert_minus','uncert_plus'],
            index=self.fuel_efs.fuel_type
            )
        
        t2t3_efs.rename(columns={'kgCO2_per_mmBtu': 'calculated'}, 
                        inplace=True)
        
        t2t3_efs['uncert_minus'] = t2t3_efs.calculated.subtract(
            t2t3_efs.calculated.multiply(t2t3_efs.final_uncert)
            )
        
        t2t3_efs['uncert_plus'] = t2t3_efs.calculated.add(
            t2t3_efs.calculated.multiply(t2t3_efs.final_uncert)
            ) 
        
        for c in ['calculated', 'uncert_plus', 'uncert_minus']:
            efs_for_energy[c].update(t2t3_efs[c])     
        
        return efs_for_energy
        
        
#%%  