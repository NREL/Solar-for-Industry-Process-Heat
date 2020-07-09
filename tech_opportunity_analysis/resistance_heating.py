#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import End_use_eff

class resistance_htg:
    
    
    def __init__(self, mfg_energy):
        
        self.frac_file = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\resistance_fractions.csv')
        
        self.bchp_frac = self.frac_file.loc[
            self.frac_file['End use'] == "Boiler/CHP"].set_index('NAICS')['fraction'].to_dict()
        
        self.ph_frac = self.frac_file.loc[
            self.frac_file['End use'] == "Process heating"].set_index('NAICS')['fraction'].to_dict()
        
        self.mfg_energy = mfg_energy
        
        self.mfg_energy.loc[:,'naics_sub'] = self.mfg_energy['naics'].astype(str).str[:3]  
        
    
    def calc_ph_energy(self):
        

        ph_energy = End_use_eff.end_use_efficiency(self.mfg_energy).ph_eu
        ph_eff = End_use_eff.end_use_efficiency(self.mfg_energy).ph_efficiency

        ph_energy.loc[:,'proc_MMBtu'] = ph_energy.apply(
            lambda x: x['MMBtu']*ph_eff*self.ph_frac.get(x['naics'],0),axis=1)
        
        
        return ph_energy
    
    
    
    
    def calc_boiler_energy(self):    
         
        
        boiler_energy = End_use_eff.end_use_efficiency(self.mfg_energy).boiler_eu
        boiler_eff = End_use_eff.end_use_efficiency(self.mfg_energy).boiler_efficiency
        
        
        #calculating proc energy based on boiler (fuel type) efficiency and inventory limit of boiler capacity
        boiler_energy.loc[:, 'proc_MMBtu'] = boiler_energy.apply(
            lambda x: boiler_eff[x['MECS_FT']] * x['MMBtu'] * self.bchp_frac.get(x['naics'],0), axis=1 )
        
        
        return boiler_energy
    
    
    
    
    def calc_chp_energy(self):
        
        
        
        chp_energy = End_use_eff.end_use_efficiency(self.mfg_energy).chp_eu
        
        chp_eff = End_use_eff.end_use_efficiency(self.mfg_energy).chp_efficiency
        
        chp_cty_list, cty_list_st, cty_list_gas, cty_list_both, cty_dict_st, cty_dict_gas,chp_st_dict, chp_gas_dict =             End_use_eff.end_use_efficiency.chp_pm_frac()
        
        
        #counties with only steam turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_st['FIPS County']),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['MMBtu'] * chp_eff['Boiler/Steam Turbine'] * self.bchp_frac.get(x['naics'],0), axis=1)

        #counties with only gas turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_gas['FIPS County']),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['MMBtu'] * chp_eff['Gas Turbine'] * self.bchp_frac.get(x['naics'],0), axis=1)

        #counties with both
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_both['FIPS County']),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['MMBtu'] * (cty_dict_st.get(x['COUNTY_FIPS'],0) * chp_eff['Boiler/Steam Turbine'] + \
                    cty_dict_gas.get(x['COUNTY_FIPS'],0)*chp_eff['Gas Turbine']) * self.bchp_frac.get(x['naics'],0), axis=1)  

        #counties with chp not mapped, so use naics subsector
        chp_energy.loc[~chp_energy['COUNTY_FIPS'].isin(chp_cty_list),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['MMBtu'] * (chp_st_dict.get(x['naics_sub'],0)*chp_eff['Boiler/Steam Turbine'] + \
                    chp_gas_dict.get(x['naics_sub'],0)*chp_eff['Gas Turbine']) * self.bchp_frac.get(x['naics'],0), axis=1)
        
        return chp_energy
    
    
    
    
    
    def combine_ph_boiler_chp(self, ph_energy, boiler_energy, chp_energy):
        
        res_process_energy = pd.concat([ph_energy, boiler_energy, chp_energy], ignore_index=False, sort=True).sort_index()
        
        return res_process_energy


# In[ ]:




