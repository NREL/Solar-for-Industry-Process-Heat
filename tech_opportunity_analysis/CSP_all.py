#!/usr/bin/env python
# coding: utf-8

# In[5]:





# In[8]:


import pandas as pd
import numpy as np
import End_use_eff

class CSP:
    
    
    def __init__(self, mfg_energy):
        
        self.ptc_upper_temp = 340  #Rackam ptc process temp
        
        self.LF_upper_temp = 212   #SAM field outlet temp
        
        self.mfg_energy = mfg_energy
        
        self.mfg_energy.loc[:,'naics_sub'] = self.mfg_energy['naics'].astype(str).str[:3]
        
    
    def calc_ph_energy(self):
        
        ph_energy = End_use_eff.end_use_efficiency(self.mfg_energy).ph_eu
        ph_eff = End_use_eff.end_use_efficiency(self.mfg_energy).ph_efficiency
        
        
        #filter based on process temp < temp limit for PTC
        ph_energy.loc[ph_energy['Temp_C'] <= self.ptc_upper_temp, 'proc_MMBtu'] = ph_energy['MMBtu']*ph_eff
        
        
        return ph_energy
    
    
    
    
    def calc_boiler_energy(self, csp_type):    
         
        if (csp_type == 'PTC'):
            mfg_energy_df = self.mfg_energy.loc[self.mfg_energy['Temp_C'] <= self.ptc_upper_temp].copy()
        elif (csp_type == 'LF'):
            mfg_energy_df = self.mfg_energy.loc[self.mfg_energy['Temp_C'] <= self.LF_upper_temp].copy()
        
        boiler_energy = End_use_eff.end_use_efficiency(mfg_energy_df).boiler_eu
        boiler_eff = End_use_eff.end_use_efficiency(mfg_energy_df).boiler_efficiency
        
        
        #calculating proc energy based on boiler (fuel type) efficiency and inventory limit of boiler capacity
        boiler_energy.loc[:, 'proc_MMBtu'] = boiler_energy.apply(
            lambda x: boiler_eff[x['MECS_FT']]*x['MMBtu'], axis=1 )
        
        
        return boiler_energy
    
    
    
    
    def calc_chp_energy(self, csp_type):
        
        if (csp_type == 'PTC'):
            mfg_energy_df = self.mfg_energy.loc[self.mfg_energy['Temp_C'] <= self.ptc_upper_temp].copy()
        elif (csp_type == 'LF'):
            mfg_energy_df = self.mfg_energy.loc[self.mfg_energy['Temp_C'] <= self.LF_upper_temp].copy()
        
        
        
        chp_energy = End_use_eff.end_use_efficiency(mfg_energy_df).chp_eu
        
        chp_eff = End_use_eff.end_use_efficiency(mfg_energy_df).chp_efficiency
        
        chp_cty_list, cty_list_st, cty_list_gas, cty_list_both, cty_dict_st, cty_dict_gas,chp_st_dict, chp_gas_dict = End_use_eff.end_use_efficiency.chp_pm_frac()
        
       
        #counties with only steam turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_st['FIPS County']),
                            'proc_MMBtu'] = chp_energy['MMBtu']*chp_eff['Boiler/Steam Turbine']  

        #counties with only gas turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_gas['FIPS County']),
                            'proc_MMBtu'] = chp_energy['MMBtu']*chp_eff['Gas Turbine']  

        #counties with both
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_both['FIPS County']),
                    'proc_MMBtu'] = chp_energy.apply(lambda x: x['MMBtu']*(
                    cty_dict_st.get(x['COUNTY_FIPS'],0)*chp_eff['Boiler/Steam Turbine'] + \
                    cty_dict_gas.get(x['COUNTY_FIPS'],0)*chp_eff['Gas Turbine']), axis=1)  

        #counties with chp not mapped, so use naics subsector
        chp_energy.loc[~chp_energy['COUNTY_FIPS'].isin(chp_cty_list),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['MMBtu']*(
            chp_st_dict.get(x['naics_sub'],0)*chp_eff['Boiler/Steam Turbine'] + \
            chp_gas_dict.get(x['naics_sub'],0)*chp_eff['Gas Turbine']), axis=1)
        
        return chp_energy
    
    
    
    
    
    def combine_ph_boiler_chp(self, ph_energy, boiler_energy, chp_energy):
        
        csp_process_energy = pd.concat([ph_energy, boiler_energy, chp_energy], ignore_index=False, sort=True).sort_index()
        
        #remove lines that have no proces heat demand
        csp_process_energy = csp_process_energy.loc[csp_process_energy['proc_MMBtu']>0].copy()
        
        return csp_process_energy
    
    
    def combine_boiler_chp(self, boiler_energy, chp_energy):
        
        csp_process_energy = pd.concat([boiler_energy, chp_energy], ignore_index=False, sort=True).sort_index()
        
        #remove lines that have no proces heat demand
        csp_process_energy = csp_process_energy.loc[csp_process_energy['proc_MMBtu']>0].copy()
        
        return csp_process_energy
    


# In[ ]:




