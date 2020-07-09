#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np


class end_use_efficiency:

    def __init__(self,mfg_energy):
        
        
        self.boiler_efficiency = {'Natural_gas': 0.75, 'LPG_NGL': 0.82, 'Diesel': 0.83,
                           'Coal': 0.81, 'Residual_fuel_oil': 0.83, 'Coke_and_breeze': 0.70,
                           'Other': 0.70 }
                            #efficiency values based on Walker, M. et al. 2013. and IEA report
        
         
        self.boiler_eu = mfg_energy.loc[mfg_energy['End_use'] == "Conventional Boiler Use"].copy()
        
        
        
        self.chp_efficiency = {'Boiler/Steam Turbine': 0.735,'Gas Turbine': 0.3942 }
                            #efficiency values based on DOE CHP fact sheets, average values among diff capacity sizes
        
         
        self.chp_eu = mfg_energy.loc[mfg_energy['End_use'] == "CHP and/or Cogeneration Process"].copy()
        
        
        self.ph_efficiency = 0.5 #based on Improving Process Heating System Performance, DOE, AMO, 2016
        
        
        self.ph_eu = mfg_energy.loc[mfg_energy['End_use'] == "Process Heating"].copy()
        
        
    def chp_pm_frac():
    
        #chp-mapped-to-county database for determining prime mover breakdown within county
        chp_cty = pd.read_csv(
        r'C:\Users\Carrie Schoeneberger\Gitlocal\Solar-for-Industry-Process-Heat\CHP\CHP with Counties_2012_NAICS_updated.csv')

        #consider only steam turbines and gas turbines, which make up 95% of capacity
        chp_cty = chp_cty.loc[(chp_cty['Prime Mover']=='Boiler/Steam Turbine') | 
                              (chp_cty['Prime Mover']=='Combined Cycle') |
                              (chp_cty['Prime Mover']=='Combustion Turbine')].copy()

        chp_cty_pm = chp_cty.replace({'Prime Mover': 
                                  {'Combined Cycle': 'Gas Turbine', 'Combustion Turbine': 'Gas Turbine'}})
        
        
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
        
        
        
        
        #chp database for determining prime mover breakdown within naics subsector
        chp_db_file = pd.read_csv('CHPDB_database.csv',thousands=',')
        
        chp_df = chp_db_file.loc[:,('Prime Mover','Capacity (kW)','NAICS')]
        
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
        
        return chp_county_list, county_list_st, county_list_gas, county_list_both,                 county_chp_st_dict, county_chp_gas_dict, chp_st_dict, chp_gas_dict
    
        
        
        
        


# In[ ]:




