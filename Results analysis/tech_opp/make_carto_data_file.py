# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 21:11:15 2020

@author: wxi

Carto CSV Data File Generation Script

"""

import pandas as pd
import numpy as np
import h5py

heat = pd.read_parquet('mfg_eu_temps_20200826_2224.parquet.gzip')
carto_data = pd.DataFrame(index = heat["COUNTY_FIPS"].unique())
heat["Temp_C"] = heat["Temp_C"].apply(float)

#Process Heat Demand Columns
heat90 = heat[heat["Temp_C"] <= 90].groupby(["COUNTY_FIPS"])[["MMBtu"]].sum().rename(columns = {"MMBtu": "Process Load Temp <=90 C (TBtu)"})/10**6
heat150 =  heat[(heat["Temp_C"] <= 150) & (heat["Temp_C"] > 90)].groupby(["COUNTY_FIPS"])[["MMBtu"]].sum().rename(columns = {"MMBtu": "Process Load Temp >90C & <=150C (TBtu)"})/10**6
heat300 = heat[(heat["Temp_C"] <= 300) & (heat["Temp_C"] > 150)].groupby(["COUNTY_FIPS"])[["MMBtu"]].sum().rename(columns = {"MMBtu": "Process Load Temp >150C & <=300C (TBtu)"})/10**6
heat500 = heat[(heat["Temp_C"] <= 500) & (heat["Temp_C"] > 300)].groupby(["COUNTY_FIPS"])[["MMBtu"]].sum().rename(columns = {"MMBtu": "Process Load Temp >300C & <=500C (TBtu)"})/10**6
heat501 = heat[heat["Temp_C"] > 500].groupby(["COUNTY_FIPS"])[["MMBtu"]].sum().rename(columns = {"MMBtu": "Process Load Temp >500 C (TBtu)"})/10**6

# missing values
dfs = [heat90, heat150, heat300, heat500, heat501]
for df in dfs:
    carto_data = carto_data.merge(df, how = "outer", left_index = True, right_index = True)
carto_data.fillna(0, inplace = True)

# resource data
filepath = "pv_sc0_t0_or0_d0_gen_2014.h5"
data = h5py.File(filepath, "r")
gid_to_fips = pd.read_csv("county_center.csv", usecols=['gid', 'FIPS'])

# matching county fips and converting gni/dni to (kwh/m^2_/year
metagid = [i[10] for i in np.array(data['meta'])]
metafips = [gid_to_fips[gid_to_fips["gid"] == i]["FIPS"].values[0] for i in metagid]
dni = np.array(data["dn"]).T
ghi = np.array(data["gh"]).T
dni_annual = np.apply_along_axis(sum, 1, dni)/2000
ghi_annual = np.apply_along_axis(sum, 1, ghi)/2000
resource = pd.DataFrame(index = metafips)
resource["dni"] = dni_annual
resource["ghi"] = ghi_annual
#var to store counties not in intersection
symdiff = set(resource.index) ^ set(carto_data.index)
carto_data = carto_data.merge(resource, how = "outer", left_index = True, right_index = True)

# tech opp results
tech = ["DSG_LF", "PTC_NOTES", "PTC_TES", "SWH", "PV_BOILER", "PV_WHRHP", "PV_RESIST"]
paths = ["dsg_lf_sizing_12_20201103_0117.hdf5", "ptc_notes_sizing_12_20201101_0300.hdf5", "ptc_tes_sizing_12_20201101_0121.hdf5", 
        "swh_sizing_12_20201101_1700.hdf5", "pv_boiler_sizing_12_20201104_0328.hdf5", "pv_whrhp_sizing_12_20201105_0027.hdf5", "pv_resist_sizing_12_20201106_0159.hdf5"]

for i,j in zip(tech, paths):
    oppdata = h5py.File(j, "r")
    df_list = []
    for key in list(oppdata.keys())[:1]:
        df_list.append(pd.DataFrame(np.array(oppdata[key]).squeeze()))
    df = pd.concat(df_list, axis = 1, sort = False)
    df.rename({0: 'industries'}, axis = 'columns', inplace = True)
    df_size = len(df)
    df[i + " % of Year County Demand Met"] = np.sum(np.array(oppdata["ophours_mean"]["tech_opp"]).T >= 1, axis = 1)/8760*100
    df.set_index("COUNTY_FIPS", inplace = True)
    df.drop(columns  = ["avail_land", "timezone"], inplace = True)
    carto_data = carto_data.merge(df, how = "outer", left_index = True, right_index = True)

# deal with missing values
carto_data = carto_data[carto_data["Process Load Temp <=90 C (TBtu)"].notna()]
exclcol = [i + " % of Year County Demand Met" for i in tech]
carto_data.dropna(subset = exclcol, how = "all", inplace = True)
carto_data.to_csv("siph_carto_data_county_res.csv")
