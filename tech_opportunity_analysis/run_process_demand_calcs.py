#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
Run process demand calcs:

    Bring in mfg eu temps data
    Calculate process energy for electric boiler, FPC, PTC, LFwDSG, resistance heating, WHR heat pump

"""

import pandas as pd
import numpy as np
import pyarrow

mfg_eu_temps = pd.read_parquet('mfg_eu_temps_20200826_2224.parquet.gzip', engine='pyarrow') 




import Elec_boiler

eb_df = Elec_boiler.electric_boiler()

eb_proc_energy = eb_df.calc_eboiler_process_energy(mfg_eu_temps)


eb_proc_energy.to_csv('eboiler_process_energy.csv.gz', compression='gzip')




import FPC_hw_heating

fpc_df = FPC_hw_heating.fpc_hot_water()

fpc_hw_energy = fpc_df.calc_hw_energy(mfg_eu_temps)



fpc_b_energy = fpc_df.calc_boiler_process_energy(fpc_hw_energy)

fpc_chp_energy = fpc_df.calc_chp_process_energy(fpc_hw_energy)

fpc_proc_energy = fpc_df.combine_boiler_chp(fpc_b_energy,fpc_chp_energy)



fpc_proc_energy.to_csv('fpc_hw_process_energy.csv.gz', compression='gzip')





import CSP_all

csp_df = CSP_all.CSP(mfg_eu_temps)



ptc_ph_energy = csp_df.calc_ph_energy()

ptc_b_energy = csp_df.calc_boiler_energy('PTC')

ptc_chp_energy = csp_df.calc_chp_energy('PTC')

ptc_proc_energy = csp_df.combine_ph_boiler_chp(ptc_ph_energy,ptc_b_energy,ptc_chp_energy)



ptc_proc_energy.to_csv('ptc_process_energy.csv.gz',compression='gzip')





LF_b_energy = csp_df.calc_boiler_energy('LF')

LF_chp_energy = csp_df.calc_chp_energy('LF')

LF_proc_energy = csp_df.combine_boiler_chp(LF_b_energy,LF_chp_energy)



LF_proc_energy.to_csv('LF_process_energy.csv.gz',compression='gzip')




import resistance_heating

res_df = resistance_heating.resistance_htg(mfg_eu_temps)

res_ph_energy = res_df.calc_ph_energy()

res_b_energy = res_df.calc_boiler_energy()

res_chp_energy = res_df.calc_chp_energy()

res_proc_energy = res_df.combine_ph_boiler_chp(res_ph_energy,res_b_energy,res_chp_energy)



res_proc_energy.to_csv('resistance_process_energy.csv.gz',compression='gzip')











import whr_heat_pump

whr_df = whr_heat_pump.WHR_HP(mfg_eu_temps)

whr_proc_energy = whr_df.calc_process_energy()



whr_proc_energy.to_csv('whr_hp_process_energy.csv.gz',compression='gzip')






