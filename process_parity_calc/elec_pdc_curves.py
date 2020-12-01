# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 03:07:54 2020

@author: wxi

Script to process electricity curves for wholesale electricity markets

"""



import webbrowser
import time
import datetime 
import json
import pandas as pd
import numpy as np

# Download CAISO node script
# =============================================================================
# def download_year(query,startdate,enddate,marketid,node):
#     baseurl = "http://oasis.caiso.com/oasisapi/SingleZip?"
#     queryname = "queryname=" + query
#     startdatetime = "startdatetime=" + startdate
#     enddatetime = "enddatetime=" + enddate
#     version = "version=1"
#     resultformat = "resultformat=6"
#     market = "market_run_id=" + marketid
#     elecnode = "node=" + node
#     return baseurl + "&".join([queryname,startdatetime,enddatetime,version,resultformat,market,elecnode])
# 
# #start and end dates
# start = [datetime.datetime(2019,1,1,0)]
# end = [datetime.datetime(2019,1,31,0)]
# for i in range(12):
#     start.append(start[i] + datetime.timedelta(days=30))
#     end.append(end[i] + datetime.timedelta(days=30))
# start = [i.strftime("%Y%m%dT%H:00-0000") for i in start]
# end = [i.strftime("%Y%m%dT%H:00-0000") for i in end]
# 
# #adjust last date in end
# end[-1] = datetime.datetime(2020,1,1,0).strftime("%Y%m%dT%H:00-0000")
# 
# #script to download all the files necessary from caiso
# for i in range(len(start)):
#     webbrowser.open(download_year("PRC_LMP", start[i], end[i], "DAM", "VIERRA_1_N001"))
#     time.sleep(10)
# =============================================================================
def main():
    path = "./eleclmp/"
    df = pd.read_csv(path + "state_list.csv", index_col = "State")
    pdc_dict = {}
    
    #ADD ISONE
    #single states 
    isone_dict = {"CT": "4004", "ME": "4001", "NH": "4002", "RI" : "4005", "VT": "4003", "NEMA" : "4008", "SEMA": "4006", "WCMA":"4007"}
    datelist = ["202001", "202002", "202003", "201904", "201905", "201906", "201907", "201908", "201909", "201910", "201911", "201912"]

    for i in isone_dict.keys():
        isone_pdc = []
        # create the state pdc curve
        for j in datelist:
            isone_data = pd.read_csv(path + "ISONE/" + str(i) + "/whlsecost_hourly_" + isone_dict[i] + "_" + j + ".csv",
                                    header = 4, usecols = ["Local Date", "Local Hour", "RTLMP"])
            # remove unnecessary rows
            isone_data.drop([0], inplace =True)
            isone_data.drop([isone_data.tail(1).index[0]], inplace = True)
            isone_data.reset_index(inplace = True, drop = True)
            
            if j == "202002":
                #drop 29th day in february for 2020 data
                isone_data.drop(list(isone_data.tail(24).index), inplace = True)
                    
            if j == "201911":
                # drop extra row
                isone_data.drop(isone_data[isone_data["Local Hour"] == "02X"].index, inplace = True)
                
    
            temp_list = list(isone_data["RTLMP"].astype("float"))
                         
            if j == "202003":
                # missing value average of two before it
                temp_list.insert(170, (float(temp_list[169]) + float(temp_list[170]))/2)
                
            isone_pdc.extend(temp_list)
    
    
        pdc_dict[i] = isone_pdc
    
    # missing data - march 8 2020 for every state, bad line 11 3 2019 each state       
    # create MA average:
    pdc_dict["MA"] = [(a+b+c)/3 for a,b,c in zip(pdc_dict["SEMA"], pdc_dict["NEMA"], pdc_dict["WCMA"])]
    
    # Add ERCOT - missing data point in march and fixed below
    ercot_data = pd.read_excel(path + "ERCOT/ERCOT.xlsx", sheet_name = None)
    ercot_pdc = []
    for key in ercot_data.keys():
        # already ordered by hour ending so no need to convert to datetime to sort
        ercot_pdc.extend(list(ercot_data[key].groupby(['Delivery Date', 'Hour Ending']).mean().reset_index()["Settlement Point Price"]))
    # fix missing value using average
    ercot_pdc.insert(1658, (ercot_pdc[1657] + ercot_pdc[1658])/2)
    # add state keys to pdc_dict
    ercot_st = df[df["Company"] == "ERCOT"].index.values
    for i in ercot_st:
        pdc_dict[i] = ercot_pdc
        
    #ADD CAISO - representative county currently
    caiso_pdc = []
    for i in range(1,14):
        caiso_data = pd.read_csv(path + "CAISO/CAISO_" + str(i) + ".csv")
        caiso_data = caiso_data.sort_values("INTERVALSTARTTIME_GMT")
        caiso_data = caiso_data[caiso_data["LMP_TYPE"] == 'LMP']
        caiso_pdc.extend(list(caiso_data["MW"]))
        
    caiso_st = df[df["Company"] == "CAISO"].index.values
    for i in caiso_st:
        pdc_dict[i] = caiso_pdc  
    
    # ADD MISO
    miso_pdc = []
    for i in ["Jan-Mar", "Apr-Jun", "Jul-Sep", "Oct-Dec"]:
        miso_data = pd.read_csv(path + "MISO/2019_" + i + "_RT_LMPs_MISO/2019_" + i + "_RT_LMPs.csv")
        col_fix = miso_data.columns[4::].values
        
        # convert to date time to sort
        miso_data["MARKET_DAY"] = pd.to_datetime(miso_data["MARKET_DAY"])
        miso_data = miso_data[miso_data["VALUE"] == "LMP"].sort_values("MARKET_DAY").reset_index(drop = True)
        
        # force bad hyphen chars that don't turn out to be negative to nan as negative LMPS don't make sense
        for col in col_fix:
            miso_data[col] = miso_data[col].apply(pd.to_numeric, errors='coerce')
        
        # average out by all nodes 
        miso_data = miso_data.groupby("MARKET_DAY").mean().values.flatten()
        print(len(miso_data))
        miso_pdc.extend(list(miso_data))
        
    miso_st = df[df["Company"] == "MISO"].index.values
    for i in miso_st:
        pdc_dict[i] = miso_pdc
        
    # ADD NYISO
    nyiso_data = pd.read_csv(path + "NYISO/NYISO.csv")
    nyiso_data.drop(["Eastern Date Hour Time Zone", "Zone Name", "Zone PTID", "DAM Zonal Losses", "DAM Zonal Congestion", "DAM Zonal Price Version"], axis = 1, inplace = True)
    nyiso_data["Eastern Date Hour"] = pd.to_datetime(nyiso_data["Eastern Date Hour"])
    nyiso_pdc = list(nyiso_data.groupby("Eastern Date Hour").mean()["DAM Zonal LBMP"])
    
    # missing first hour data
    nyiso_pdc.insert(0, nyiso_pdc[0])
    pdc_dict["NY"] = nyiso_pdc
    
    # ADD PJM
    pjm_data = pd.read_csv(path + "PJM/PJM_2019_data.csv", usecols = ["datetime_beginning_utc", "pnode_name", "total_lmp_rt"])
    pjm_data["datetime_beginning_utc"] = pd.to_datetime(pjm_data["datetime_beginning_utc"])
    pjm_list = list(pjm_data.groupby("datetime_beginning_utc").mean()["total_lmp_rt"])
    
    #PJM data missing first 5 and last 18 data points - replace with values for the day 1 week later or earlier
    pjm_pdc = pjm_list[163:168] + pjm_list + pjm_list[8569:8569+18]
    pjm_st = df[df["Company"] == "PJM"].index.values
    for i in pjm_st:
        pdc_dict[i] = pjm_pdc
        
    #ADD SPP
    spp_pdc = []
    for i in range(1,13):
        spp_data = pd.read_csv(path + "SPP/SPP_" + str(i) + ".csv")
        # convert datetime to sort
        spp_data["Date"] = pd.to_datetime(spp_data["Date"])
        
        # filter out lmp only
        spp_data = spp_data[spp_data[" Price Type"] == "LMP"]
        
        # doesnt have full 24hs so has nan values but covered by other files
        temp_list = spp_data.groupby(["Date"]).mean().values.flatten()
        temp_list = list(temp_list[~np.isnan(temp_list)])
        spp_pdc.extend(temp_list)
        
    spp_st = df[df["Company"] == "SPP"].index.values
    for i in spp_st:
        pdc_dict[i] = spp_pdc
        
    #default average eia electricity price for rest of states 
    #https://www.statista.com/statistics/190680/us-industrial-consumer-price-estimates-for-retail-electricity-since-1970/
    for i in df[df["Company"] == "DEFAULT"].index:
        pdc_dict[i] = [68.3 for i in range(8760)]
    
    with open('elec_curve.json', 'w') as f:
        json.dump(pdc_dict, f)