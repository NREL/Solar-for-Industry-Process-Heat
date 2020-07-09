#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import End_use_eff

class electric_boiler:

    def __init__(self):
        
        
        
        self.eboiler_max_cap = 190 #in MMBtu/hr
        
        self.eboiler_factor = 1 #represents number of e-boilers that could replace conventional
        
        #percent of installed boiler capacity <= max eboiler capacity 
        self.percent_cap_replace = {'311': 0.18614*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.2388,
                                    '312': 0.18614*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.2388,
                                    '313': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '314': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '315': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '316': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '321': 0.22585*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.43429,
                                    '322': -5*10**(-7)*(self.eboiler_max_cap*self.eboiler_factor)**2 + \
                                            0.0014*(self.eboiler_max_cap*self.eboiler_factor)+0.0409,
                                    '324': -9*10**(-7)*(self.eboiler_max_cap*self.eboiler_factor)**2 + \
                                            0.002*(self.eboiler_max_cap*self.eboiler_factor)-0.04658,
                                    '325': 0.21192*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.49737,
                                    '326': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '327': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '331': 0.19494*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.45473,
                                    '332': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '333': 0.20511*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.3591,
                                    '334': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '335': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '336': 0.20511*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.3591,
                                    '337': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453,
                                    '339': 0.25212*np.log(self.eboiler_max_cap*self.eboiler_factor)-0.52453
                                   }
                                    # equations based on e-boiler technical potential analysis
       
                                    
    def calc_eboiler_process_energy(self, mfg_energy):    
         
        boiler_energy = End_use_eff.end_use_efficiency(mfg_energy).boiler_eu
        boiler_eff = End_use_eff.end_use_efficiency(mfg_energy).boiler_efficiency
        
        boiler_energy.loc[:,'naics_sub'] = boiler_energy['naics'].astype(str).str[:3]
        
        #calculating proc energy based on efficiency only, need to ADD limit of capacity
        boiler_energy.loc[:,'proc_MMBtu'] = boiler_energy.apply(
            lambda x: boiler_eff[x['MECS_FT']] * self.percent_cap_replace[x['naics_sub']] * x['MMBtu'], axis=1 )
        
        return boiler_energy


# In[ ]:




