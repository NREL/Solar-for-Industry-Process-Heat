# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 14:32:57 2020

@author: wxi
"""
import os
import pandas as pd


class ModelParams:
    
     path = "./calculation_data"
     hv_vals = {"PETRO" : 4.641 * 1055055.85 / 42, "NG" : 1039 * 1.05506 / 0.001, 
                "COAL" : 20.739 * 1055055.85}
     month = 6
     fips_data = pd.read_csv(os.path.join(path, "US_FIPS_Codes.csv"), usecols=['State', 'COUNTY_FIPS', 'Abbrev']) 
     chp = "gas"
     # techopp, peakload, annual (perfect demand supply match) - max sizing
     sizing = "peakload"
     #caps only
     furnace = "REVERB"
     boilereff = False
     #flue gas monitoring/process control, automatic steam trap monitoring, economizer
     boilereffcap = [84, 250, 20913]
     boilereffom = [0, 2.5, 0]
     boilereffinc = [0.0109, 0.0882/100, 0.0914]
     
     gen_dict= {"DSGLF": "dsg_lf_gen.json", "PTC": "ptc_notes_gen.json", 
                "PTCTES": "ptc_tes_gen.json", "SWH": "swh_gen.json",
                "PVEB": "pv_ac_gen.json", "PVRH": "pv_dc_gen.json"}
     # on or off for depreciation
     deprc = True

     @classmethod
     def get_state_names(cls, county):
         state_name = ModelParams.fips_data.loc[ModelParams.fips_data['COUNTY_FIPS'] == county,'State'].values[0].strip()
         state_abbr = ModelParams.fips_data.loc[ModelParams.fips_data['COUNTY_FIPS'] == county,'Abbrev'].values[0].strip()
         return (state_name, state_abbr)


