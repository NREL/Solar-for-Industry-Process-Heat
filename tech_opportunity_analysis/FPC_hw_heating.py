#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pandas as pd
import numpy as np

import End_use_eff


# In[10]:


class fpc_hot_water:
    
    def __init__(self):
        
    
       
        #determine fraction of hot water use by naics subsector
        
        self.hw_steam_frac = pd.read_csv('hw_steam_fraction_NAICS.csv')


        self.hw_only = self.hw_steam_frac.loc[(self.hw_steam_frac.og_HT == "water")].copy().reset_index()


        self.hw_only.loc[:,'count'] = self.hw_only.groupby('NAICS12')['NAICS12'].transform('count') 


        self.hw_sum = self.hw_only.loc[:,'%_Btu'].groupby([self.hw_only['NAICS12'],self.hw_only['count']]).sum().reset_index()


        self.hw_sum.loc[:,'naics_sub'] = self.hw_sum.NAICS12.astype(str).str[:3]


        self.hw_sub = self.hw_sum.groupby('naics_sub').apply(
            lambda x: np.average(x['%_Btu'], weights=x['count']))#.reset_index(name='%_hw')


        self.hw_frac = self.hw_sub.to_dict()



    def calc_hw_energy(self, mfg_energy):
        
        boiler_chp_eu = mfg_energy.loc[(mfg_energy['End_use'] =='Conventional Boiler Use') | 
                      (mfg_energy['End_use']=='CHP and/or Cogeneration Process')].copy()
        
        
        
        boiler_chp_eu.loc[:,'naics_sub'] = (boiler_chp_eu.loc[:,'naics']//1000).astype('str')
        
        
        mfg_hw = boiler_chp_eu.loc[boiler_chp_eu['naics_sub'].isin(['311','312','313','325','333','336'])].copy()  

 
        mfg_hw.loc[:,'hw_MMBtu'] = mfg_hw.apply(lambda x: self.hw_frac[x['naics_sub']]*x['MMBtu'], axis=1)
    
        return mfg_hw
    

    def calc_boiler_process_energy(self, mfg_hw):    
        
        boiler_energy = End_use_eff.end_use_efficiency(mfg_hw).boiler_eu
        boiler_eff = End_use_eff.end_use_efficiency(mfg_hw).boiler_efficiency
        
        boiler_energy.loc[:,'proc_MMBtu'] = boiler_energy.apply(
            lambda x: boiler_eff[x['MECS_FT']] * x['hw_MMBtu'], axis=1 )
        
        return boiler_energy
    
    def calc_chp_process_energy(self, mfg_hw):
        
        chp_energy = End_use_eff.end_use_efficiency(mfg_hw).chp_eu
        
        chp_eff = End_use_eff.end_use_efficiency(mfg_hw).chp_efficiency
        
        chp_cty_list, cty_list_st, cty_list_gas, cty_list_both, cty_dict_st, cty_dict_gas,chp_st_dict, chp_gas_dict =             End_use_eff.end_use_efficiency.chp_pm_frac()
        
        
        #counties with only steam turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_st['FIPS County']),
                            'proc_MMBtu'] = chp_energy['hw_MMBtu']*chp_eff['Boiler/Steam Turbine']  

        #counties with only gas turbine chp
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_gas['FIPS County']),
                            'proc_MMBtu'] = chp_energy['hw_MMBtu']*chp_eff['Gas Turbine']  

        #counties with both
        chp_energy.loc[chp_energy['COUNTY_FIPS'].isin(cty_list_both['FIPS County']),
                    'proc_MMBtu'] = chp_energy.apply(lambda x: x['hw_MMBtu']*(
                    cty_dict_st.get(x['COUNTY_FIPS'],0)*chp_eff['Boiler/Steam Turbine'] + \
                    cty_dict_gas.get(x['COUNTY_FIPS'],0)*chp_eff['Gas Turbine']), axis=1)  

        #counties with chp not mapped, so use naics subsector
        chp_energy.loc[~chp_energy['COUNTY_FIPS'].isin(chp_cty_list),'proc_MMBtu'] = chp_energy.apply(
            lambda x: x['hw_MMBtu']*(
            chp_st_dict[x['naics_sub']]*chp_eff['Boiler/Steam Turbine'] + \
            chp_gas_dict[x['naics_sub']]*chp_eff['Gas Turbine']), axis=1)
        
        return chp_energy
    
    
    def combine_boiler_chp(self, boiler_energy, chp_energy):
        
        fpc_process_energy = pd.concat([boiler_energy, chp_energy], ignore_index=False, sort=True).sort_index()
        
        return fpc_process_energy
        


    


# In[ ]:




