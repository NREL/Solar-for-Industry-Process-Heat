# -*- coding: utf-8 -*-
"""
Created on Wed Sep 23 15:23:16 2020

@author: wxi
"""
import pandas as pd
import numpy as np
import h5py

def get_json(filepath, techname):
    data = h5py.File("./calculation_data/" + filepath, "r")
    generation_groups = {
    'ptc_tes': {'power': ['q_dot_to_heat_sink'],  # in MWt
                'footprint': 16187},
    'dsg_lf': {'power': ['q_dot_to_heat_sink'],  # in MWt
               'footprint': 3698},
    'ptc_notes': {'power': ['q_dot_to_heat_sink'],  # in MWt
                  'footprint': 8094},
    'pv_ac': {'power': ['ac'], 'footprint': 35208},  # 1-axis; in W
    'pv_dc': {'power': ['dc'], 'footprint': 42250},  # 1-axis; in W
    'swh': {'power': ['Q_deliv'], 'footprint': 2024}  # in kWth
            }
    generation = np.array(data[generation_groups[techname]['power'][0]]).T
    gen = np.zeros((generation.shape[0], 8760))
    
    for idx in range(len(generation)):  
        if idx%100 == 0:
            print(idx)     
        gen[idx] = np.array([sum(generation[idx][i:i + 2]) for i in range(0, len(generation[idx]), 2)])
    
    gid_to_fips = pd.read_csv("./calculation_data/county_center.csv", usecols=['gid', 'FIPS'])
    
    metanames = [i[6].decode("utf-8")  for i in np.array(data['meta'])]
    metagid = [i[10] for i in np.array(data['meta'])]
    metafips = [gid_to_fips[gid_to_fips["gid"] == i]["FIPS"].values[0] for i in metagid]
    metatzone = [i[3] for i in np.array(data['meta'])]
    
    solar_gen = pd.DataFrame()

    if techname in ['ptc_tes', 'dsg_lf', 'ptc_notes']:
        solar_gen["gen"] = list(gen*1000)
        
    elif techname in ['swh']:
        solar_gen["gen"] = list(gen)
    else:
        solar_gen["gen"] = list(gen/1000)
    
    solar_gen["timezone"] = metatzone
    
    solar_gen["FIPS"] = metafips
    
    solar_gen["gen"] = solar_gen.apply(lambda x: np.roll(x['gen'], x['timezone']), axis=1)
    
    def monthly_sum(a):
        length = np.array([31,28,31,30,31,30,31,31,30,31,30,31]) * 24
        start = np.array([0,31,59,90,120,151,181,212,243,273,304,334])*24
        monthlysums = [sum(a[start[i] : start[i] + length[i]]) for i in range(12)]
        return monthlysums
    
    solar_gen["monthlysums"] = solar_gen["gen"].apply(monthly_sum)
    
    solar_gen.to_json("./calculation_data/" + techname + "_gen.json")
    
def is_sun():
    data = h5py.File("./calculation_data/" + "pv_sc0_t0_or0_d0_gen_2014.h5", "r")
    solar = pd.DataFrame()
    arr = np.array(data["sunup"]).T
    sun = np.zeros((arr.shape[0], 8760))
    for idx in range(len(arr)):  
        if idx%100 == 0:
            print(idx)     
            sun[idx] = np.array([1 if any(arr[idx][i:i + 2]) else 0 for i in range(0, len(arr[idx]), 2)])
    
    gid_to_fips = pd.read_csv("./calculation_data/county_center.csv", usecols=['gid', 'FIPS'])

    metagid = [i[10] for i in np.array(data['meta'])]
    metafips = [gid_to_fips[gid_to_fips["gid"] == i]["FIPS"].values[0] for i in metagid]
    metatzone = [i[3] for i in np.array(data['meta'])]
    
    solar["timezone"] = metatzone
    solar["FIPS"] = metafips
    solar["sun"] = list(arr)
    solar["sun"] = solar.apply(lambda x: np.roll(x['sun'], x['timezone']), axis=1)
    
    solar.to_json("./calculation_data/issun.json")
    
if __name__ == "__main__":
# =============================================================================
#     #script for json file generation
#     techname = ["ptc_tes", "dsg_lf", "ptc_notes", "swh", "pv_ac", "pv_dc"]
#     filepaths = ["ptc_tes6hr_sc0_t0_or0_d0_gen_2014.h5", 
#                  "dsg_lf_sc0_t0_or0_d0_gen_2014.h5", 
#                  "ptc_notes_sc0_t0_or0_d0_gen_2014.h5", 
#                  "swh_sc0_t0_or0_d0_gen_2014.h5",
#                  "pv_sc0_t0_or0_d0_gen_2014.h5"]
# 
#     for i,j in zip(filepaths, techname):
#         get_json(i,j)
#     
# =============================================================================
# =============================================================================
#     # script for is_sun check
#     is_sun()
# =============================================================================
