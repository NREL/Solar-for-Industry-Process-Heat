#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
Run process demand calcs:

    Bring in mfg eu temps data
    Calculate process energy for electric boiler, FPC, PTC, LFwDSG, resistance heating

"""

import pandas as pd
import numpy as np
import pyarrow
import tarfile

with tarfile.open('mfg_eu_temps_20191003.tar.gz', mode='r:gz') as tf:
    tf.extractall('c:/users/carrie schoeneberger/Gitlocal')
    mfg_eu_temps = pd.read_parquet('c:/users/carrie schoeneberger/Gitlocal/mfg_eu_temps_20191003/', engine='pyarrow') 
    


# In[20]:





# In[9]:


import Elec_boiler


# In[10]:


eb_df = Elec_boiler.electric_boiler()


# In[11]:


eb_proc_energy = eb_df.calc_eboiler_process_energy(mfg_eu_temps)


# In[13]:


eb_proc_energy.to_csv('eboiler_process_energy.csv.gz', compression='gzip')


# In[ ]:





# In[6]:


import FPC_hw_heating
fpc_df = FPC_hw_heating.fpc_hot_water()
fpc_hw_energy = fpc_df.calc_hw_energy(mfg_eu_temps)


# In[7]:


fpc_b_energy = fpc_df.calc_boiler_process_energy(fpc_hw_energy)
fpc_chp_energy = fpc_df.calc_chp_process_energy(fpc_hw_energy)
fpc_proc_energy = fpc_df.combine_boiler_chp(fpc_b_energy,fpc_chp_energy)


# In[14]:


fpc_proc_energy.to_csv('fpc_hw_process_energy.csv.gz', compression='gzip')


# In[ ]:




