#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

class WHR_HP:
    
    def __init__(self,mfg_eu_temps_df):
        
    
        
        self.wh_frac = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\WHP fraction updated.csv')
        
        self.cop = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\COP updated.csv')
        
        
        
        #create dictionaries that match naics code to WHP fraction and to COP
        self.wh_frac = self.wh_frac.loc[:,('naics', 'end use','Overall_fraction')]
        
        self.wh_frac['naics'] = self.wh_frac['naics'].astype(str)


        self.wh_frac_bchp_dict = self.wh_frac[
            self.wh_frac['end use']=="Boiler/CHP"].set_index('naics')['Overall_fraction'].to_dict()

        self.wh_frac_ph_dict = self.wh_frac[
            self.wh_frac['end use']=="Process heating"].set_index('naics')['Overall_fraction'].to_dict()
        
        

        self.cop = self.cop.loc[:,('naics', 'end use','COP')]
        
        self.cop['naics'] = self.cop['naics'].astype(str)

        self.cop_bchp_dict = self.cop[
            self.cop['end use']=="Boiler/CHP"].set_index('naics')['COP'].to_dict()

        self.cop_ph_dict = self.cop[
            self.cop['end use']=="Process heating"].set_index('naics')['COP'].to_dict()
        
        
        
        self.mfg_energy = mfg_eu_temps_df
        
        
        
    def calc_process_energy(self): 
        
        
        #filter for the boiler and chp end use
        boiler_chp = self.mfg_energy.loc[(self.mfg_energy['End_use'] =='Conventional Boiler Use') | 
                      (self.mfg_energy['End_use']=='CHP and/or Cogeneration Process')].copy()

        boiler_chp.loc[:,'naics_sub'] = boiler_chp['naics'].astype(str).str[:3]
        
        boiler_chp.loc[:,'naics'] = boiler_chp['naics'].astype(str)

        
        #filter for the process heating end use
        ph = self.mfg_energy.loc[(self.mfg_energy['End_use'] =='Process Heating')].copy()
        
        ph.loc[:,'naics_sub'] = ph['naics'].astype(str).str[:3]
        
        ph.loc[:,'naics'] = ph['naics'].astype(str)
        
        
        
        #calculate technical potential
        boiler_chp.loc[:,'proc_MMBtu'] = boiler_chp.apply(
            lambda x: x['MMBtu']*(self.wh_frac_bchp_dict.get(x['naics_sub'],0) + self.wh_frac_bchp_dict.get(x['naics'],0))*\
                (1 + 1/((self.cop_bchp_dict.get(x['naics_sub'],0)+self.cop_bchp_dict.get(x['naics'],0))-1)),axis=1)


        ph.loc[:,'proc_MMBtu'] = ph.apply(
            lambda x: x['MMBtu']*(self.wh_frac_ph_dict.get(x['naics_sub'],0) + self.wh_frac_ph_dict.get(x['naics'],0))*\
                (1 + 1/((self.cop_ph_dict.get(x['naics_sub'],0)+self.cop_ph_dict.get(x['naics'],0))-1)),axis=1)
        
        #combine into one df
        whr_process_energy = pd.concat([boiler_chp, ph], ignore_index=True)
        
        #remove lines that have no proces heat demand
        whr_process_energy = whr_process_energy.loc[whr_process_energy['proc_MMBtu']>0].copy()
        
        return whr_process_energy


# In[ ]:




