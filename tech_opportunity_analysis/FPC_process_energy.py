#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
FPC case 1: hot water heating

bring in mfg eu data
run hw calc
run end use efficiency calc, for boiler and chp each
output county mfg data with new column of process energy

"""

import pandas as pd
import numpy as np
import pyarrow
import tarfile

with tarfile.open('mfg_eu_temps_20191003.tar.gz', mode='r:gz') as tf:
    tf.extractall('c:/users/carrie schoeneberger/Gitlocal')
    mfg_eu_temps = pd.read_parquet('c:/users/carrie schoeneberger/Gitlocal/mfg_eu_temps_20191003/', engine='pyarrow')        
    

    
    
    


# In[2]:


#class for determining amount of thermal energy used for hot water heating

class hw_use:
    
    def __init__(self, mfg_energy):
        
        
        self.hw_end_uses = mfg_energy.loc[(mfg_energy['End_use'] =='Conventional Boiler Use') | 
                      (mfg_energy['End_use']=='CHP and/or Cogeneration Process')].copy()
    
    
    
    
    def calc_hw_frac():
       
        #determine fraction of hot water use by naics subsector
        
        hw_steam_frac = pd.read_csv('hw_steam_fraction_NAICS.csv')


        hw_only = hw_steam_frac.loc[(hw_steam_frac.og_HT == "water")].copy().reset_index()


        hw_only.loc[:,'count'] = hw_only.groupby('NAICS12')['NAICS12'].transform('count') 


        hw_sum = hw_only.loc[:,'%_Btu'].groupby([hw_only['NAICS12'],hw_only['count']]).sum().reset_index()


        hw_sum.loc[:,'naics_sub'] = hw_sum.NAICS12.astype(str).str[:3]


        hw_sub = hw_sum.groupby('naics_sub').apply(
            lambda x: np.average(x['%_Btu'], weights=x['count']))#.reset_index(name='%_hw')


        hw_frac = hw_sub.to_dict()

    
        return hw_frac




    def calc_hw_energy(self, hw_frac):
        
        
        boiler_chp_eu = self.hw_end_uses
        
        boiler_chp_eu.loc[:,'naics_sub'] = (boiler_chp_eu.loc[:,'naics']//1000).astype('str')
        
        
        mfg_hw = boiler_chp_eu.loc[boiler_chp_eu['naics_sub'].isin(['311','312','313','325','333','336'])].copy()  

 
        mfg_hw.loc[:,'hw_MMBtu'] = mfg_hw.apply(lambda x: hw_frac[x['naics_sub']]*x['MMBtu'], axis=1)
    
        return mfg_hw
        


# In[3]:


# run hot water use calc

mfg_hw_df = hw_use(mfg_eu_temps)

hw_fraction = hw_use.calc_hw_frac()

mfg_hw_energy = mfg_hw_df.calc_hw_energy(hw_fraction)


# In[4]:


#class for end use efficiency calcs. For FPC, that is Boiler and CHP.

class End_use_efficiency:

    def __init__(self, mfg_hw):
        
        
        self.boiler_efficiency = {'Natural_gas': 0.75, 'LPG_NGL': 0.82, 'Diesel': 0.83,
                           'Coal': 0.81, 'Residual_fuel_oil': 0.83, 'Coke_and_breeze': 0.70,
                           'Other': 0.70 }
                            #efficiency values based on Walker, M. et al. 2013. and IEA report
            
        self.boiler_eu = mfg_hw.loc[mfg_hw['End_use'] == "Conventional Boiler Use"].copy()
        
        self.chp_file = pd.read_csv('CHPDB_database.csv',thousands=',')
        
        self.chp_efficiency = {'Boiler/Steam Turbine': 0.735,'Gas Turbine': 0.3942 }
                            #efficiency values based on DOE CHP fact sheets, average values among diff capacity sizes
        
        self.chp_eu = mfg_hw.loc[mfg_hw['End_use'] == "CHP and/or Cogeneration Process"].copy()

        
    def calc_boiler_process_energy(self):    
        
        mfg_boiler = self.boiler_eu
        
        mfg_boiler.loc[:,'proc_MMBtu'] = mfg_boiler.apply(
            lambda x: self.boiler_efficiency[x['MECS_FT']] * x['hw_MMBtu'], axis=1 )
        
        return mfg_boiler
   

    
    def calc_chp_process_energy(self): 
        
        
        #chp-mapped-to-county database for determining prime mover breakdown within county
        chp_cty = pd.read_csv(
        r'C:\Users\Carrie Schoeneberger\Gitlocal\Solar-for-Industry-Process-Heat\CHP\CHP with Counties_2012_NAICS_updated.csv')

        #consider only steam turbines and gas turbines, which make up 95% of capacity
        chp_cty = chp_cty.loc[(chp_cty['Prime Mover']=='Boiler/Steam Turbine') | 
                              (chp_cty['Prime Mover']=='Combined Cycle') |
                              (chp_cty['Prime Mover']=='Combustion Turbine')].copy()

        chp_cty_pm = chp_cty.replace({'Prime Mover': 
                                  {'Combined Cycle': 'Gas Turbine', 'Combustion Turbine': 'Gas Turbine'}})
        
        
        
        #chp database for determining prime mover breakdown within naics subsector
        
        chp_df = self.chp_file.loc[:,('Prime Mover','Capacity (kW)','NAICS')]
        
        chp_df = chp_df[(chp_df.NAICS >= 311111) & (chp_df.NAICS <= 339999)]
        
        
        #consider only steam turbines and gas turbines, which make up 95% of capacity
        chp_df = chp_df[(chp_df['Prime Mover'] == 'Boiler/Steam Turbine') | 
                        (chp_df['Prime Mover'] == 'Combined Cycle') | 
                        (chp_df['Prime Mover'] == 'Combustion Turbine')]
        
        chp_pm = chp_df.replace({'Prime Mover': 
                                  {'Combined Cycle': 'Gas Turbine', 'Combustion Turbine': 'Gas Turbine'}})
        
        #determine prime mover breakdown, based on capacity, within naics subsector
        
        chp_pm['NAICS_sub'] = (chp_pm['NAICS'].astype(int)//1000).astype('str')
        
        chp_pm_grouped = chp_pm.groupby(
            ['NAICS_sub','Prime Mover'])['Capacity (kW)'].sum()/chp_pm.groupby(['NAICS_sub'])['Capacity (kW)'].sum()
        
        chp_frac= chp_pm_grouped.reset_index()
        
        
        chp_st = chp_frac[chp_frac['Prime Mover']=="Boiler/Steam Turbine"]
        chp_st = chp_st.loc[:,('NAICS_sub', 'Capacity (kW)')]
        chp_st_dict = chp_st.set_index('NAICS_sub')['Capacity (kW)'].to_dict()
        chp_st_dict['336']=0
        
        chp_gas = chp_frac[chp_frac['Prime Mover']=="Gas Turbine"]
        chp_gas = chp_gas.loc[:,('NAICS_sub', 'Capacity (kW)')]
        chp_gas_dict = chp_gas.set_index('NAICS_sub')['Capacity (kW)'].to_dict()
        
        
        
        #determine prime mover breakdown, by capacity, within each county
        
        chp_pm_county = chp_cty_pm.groupby(
            ['FIPS County','Prime Mover'])['Capacity (kW)'].sum()/chp_cty_pm.groupby(['FIPS County'])['Capacity (kW)'].sum()

        chp_county_frac = chp_pm_county.reset_index()
        
        
        #list of all counties with chp mapped
        chp_county_list = chp_cty_pm['FIPS County'].unique()
        
        #list of counties with steam turbine chp only, gas turbine only, and both
        county_list_st = chp_county_frac.loc[(
            chp_county_frac['Prime Mover']=='Boiler/Steam Turbine') & (chp_county_frac['Capacity (kW)']==1)]
        

        county_list_gas = chp_county_frac.loc[(
            chp_county_frac['Prime Mover']=='Gas Turbine') & (chp_county_frac['Capacity (kW)']==1)]


        county_list_both = chp_county_frac.loc[(chp_county_frac['Capacity (kW)']<1)]
        
        
        county_chp_st = county_list_both[county_list_both['Prime Mover']=="Boiler/Steam Turbine"]
        county_chp_st_dict = county_chp_st.loc[
            :,('FIPS County', 'Capacity (kW)')].set_index('FIPS County')['Capacity (kW)'].to_dict()


        county_chp_gas = county_list_both[county_list_both['Prime Mover']=="Gas Turbine"]
        county_chp_gas_dict = county_chp_gas.loc[
            :,('FIPS County', 'Capacity (kW)')].set_index('FIPS County')['Capacity (kW)'].to_dict()
        
        
        #determine efficiency of chp energy inputs
        
        mfg_chp = self.chp_eu
        
    
        
        #counties with only steam turbine chp
        mfg_chp.loc[mfg_chp['COUNTY_FIPS'].isin(county_list_st['FIPS County']),
                            'proc_MMBtu'] = mfg_chp['hw_MMBtu']*self.chp_efficiency['Boiler/Steam Turbine']  

        #counties with only gas turbine chp
        mfg_chp.loc[mfg_chp['COUNTY_FIPS'].isin(county_list_gas['FIPS County']),
                            'proc_MMBtu'] = mfg_chp['hw_MMBtu']*self.chp_efficiency['Gas Turbine']  

        #counties with both
        mfg_chp.loc[mfg_chp['COUNTY_FIPS'].isin(county_list_both['FIPS County']),
                    'proc_MMBtu'] = mfg_chp.apply(lambda x: x['hw_MMBtu']*(
                    county_chp_st_dict.get(x['COUNTY_FIPS'],0)*self.chp_efficiency['Boiler/Steam Turbine'] + \
                    county_chp_gas_dict.get(x['COUNTY_FIPS'],0)*self.chp_efficiency['Gas Turbine']), axis=1)  

        #counties with chp not mapped, so use naics subsector
        mfg_chp.loc[~mfg_chp['COUNTY_FIPS'].isin(chp_county_list),'proc_MMBtu'] = mfg_chp.apply(
            lambda x: x['hw_MMBtu']*(
            chp_st_dict[x['naics_sub']]*self.chp_efficiency['Boiler/Steam Turbine'] + \
            chp_gas_dict[x['naics_sub']]*self.chp_efficiency['Gas Turbine']), axis=1) 
        
        
        return mfg_chp
        
        
    def combine_boiler_chp(self, proc_en_boiler, proc_en_chp):
        
        proc_energy = pd.concat([proc_en_boiler, proc_en_chp], ignore_index=False, sort=True).sort_index()
        
        return proc_energy


# In[5]:


#run end use efficiency calcs on county mfg data with hw use energy

eu_df = End_use_efficiency(mfg_hw_energy)

proc_energy_boiler = eu_df.calc_boiler_process_energy() 
proc_energy_chp = eu_df.calc_chp_process_energy() 

mfg_process_energy = eu_df.combine_boiler_chp(proc_energy_boiler,proc_energy_chp)


# In[9]:


mfg_process_energy.to_csv('fpc_mfg_process_energy.csv')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




