# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:37:30 2020
Grabs the best estimate of the current fuel price using EIA database for
coal, petroleum and natural gas. Returns the time and fuel price.
@author: wxi
"""

import datetime
import pandas as pd
import os
import eia
import re
import requests
import json
from collections import OrderedDict

class UpdateParams:

    """
    The intention of this class is to obtain the latest LCOH parameters that
    are from online databases.
    Expects inputs of NG, PETRO or, COAL

    NG -> industrial price ($ per thousand cubic feet)
    COAL -> other industrial use price ($ per short ton)
    Petroleum -> residual fuel oil prices by area -> wholesale/resale price by
    all sellers annual ($ per gallon)
    
    CHECK UNITS BETWEEN SERIES _ HENRY HUB , etc. 

    """

    path = "./calculation_data"

    today = datetime.datetime.now()
    
    eia_api_key = "68ea6b4094e685e32ec986a8053568d9"
    
    api = eia.API(eia_api_key)
          
    eerc_esc = pd.read_csv(os.path.join(path, "EERC_Fuel_Esc.csv"), index_col = ["State"])
    
    def get_max_fp(state_abbr, fuel_type="NG", year=False):
        
        """Obtains max state-level fuel price"""
        
        if(not year):

            year = UpdateParams.today.year

        if fuel_type.upper() == "NG":

            series_ID = "NG.N3035" + state_abbr + "3.A"

        elif fuel_type.upper() == "COAL":

            series_ID = "COAL.COST." + state_abbr + "-10.A"

        elif fuel_type.upper() == "PETRO":

            series_ID = "PET.EMA_EPPR_PWA_S" + state_abbr + "_DPG.A"

        else:
            raise AssertionError("Please input a valid fuel_type")
        
        # Check if state-level available, if not return USA price
        try:
            fuel_series = UpdateParams.api.data_by_series(series=series_ID)

            dict_key = list(fuel_series.keys())[0]

            # if fuel price in state is empty return national price
            if all(v is None for v in list(fuel_series[dict_key].values())):
                
                return 0.0
                
        except KeyError:
                
            return 0.0
        
        j = 0
        
        while True:

            try:
                return fuel_series[dict_key][str(year-j) + "  "] / 1.0

                break

            except:

                j += 1
    
    def get_fuel_price(state_abbr, fuel_type="NG", year=False):

        """Obtain fuel avgs on the annul, state scale from the EIA database."""

        if(not year):

            year = UpdateParams.today.year

        if fuel_type.upper() == "NG":

            series_ID = "NG.N3035" + state_abbr + "3.A"
            
            series_USA = "NG.RNGWHHD.A"
            
            series_LA = UpdateParams.api.data_by_series(series="NG.N3035" + "LA" + "3.A")
            
            dict_key_LA = list(series_LA.keys())[0]

        elif fuel_type.upper() == "COAL":

            series_ID = "COAL.COST." + state_abbr + "-10.A"

            series_USA = "COAL.COST.US-10.A"

        elif fuel_type.upper() == "PETRO":
            # state level wholesale/resale price data ends 2011
            series_ID = "PET.EMA_EPPR_PWA_S" + state_abbr + "_DPG.A"

            series_USA = "PET.EMA_EPPR_PWG_NUS_DPG.A"

        else:
            raise AssertionError("Please input a valid fuel_type")

        fuel_series_USA = UpdateParams.api.data_by_series(series=series_USA)
        
        dict_key_USA = list(fuel_series_USA.keys())[0]
        
        # find latest USA value
        i = 0

        while True:
            
            try:
                fp_USA = fuel_series_USA[dict_key_USA][str(year-i) + "  "] / 1.0

                break

            except:
                
                i += 1

        # Check if state-level available, if not return USA price
        try:
            fuel_series = UpdateParams.api.data_by_series(series=series_ID)

            dict_key = list(fuel_series.keys())[0]

            # if fuel price in state is empty return national price
            if all(v is None for v in list(fuel_series[dict_key].values())):
                
                return (fp_USA, year-i)
                
        except KeyError:
                
            return (fp_USA, year-i)

        j = 0

        # find latest year for state
        while True:

            try:
                fp_state = fuel_series[dict_key][str(year-j) + "  "] / 1.0

                break

            except:

                j += 1
        
        if fuel_type.upper() == "NG":
            # series_LA is just the actual series not a series ID
            fp_mult = fp_state / series_LA[dict_key_LA][str(year-j) + "  "]
            return (fp_mult * fp_USA, year-j)
                
        # return USA value if 2 years more recent vs state
        if ((year-i) - (year-j) >= 2) | (fp_state >= fp_USA):
                
            return (fp_USA, year-i)

        return (fp_state, year-j)
    
    def get_esc(state_abbr, fuel_type="NG"):
    
        """Grabs fuel esc from EERC"""
        
        temp_dict = {"NG": "Natural Gas", "COAL" : "Coal", "PETRO": "Residual"}
        
        return UpdateParams.eerc_esc.loc[state_abbr, temp_dict[fuel_type]]
      
    def create_index():
        
        """ 
        https://fred.stlouisfed.org/series/WPU061 - producer price index csv
    
        https://www.chemengonline.com/pci - chemical eng cost index - by year
    
        """ 
        path = UpdateParams.path
        
        def remove_nonnumeric(string):
            dummy_var = float(re.sub(r'[^\d.]', '', string))
            return dummy_var        
        
        def get_CE_index():
        
            cost_dict = {}
        
            index_list = ["CE INDEX", "Equipment", "Heat Exchangers and Tanks", 
                          "Process Machinery", "Pipe, valves and fittings", 
                          "Process Instruments", 
                          "Pumps and Compressors", "Electrical equipment", 
                          "Structural supports", "Construction Labor", "Buildings", 
                          "Engineering Supervision"]
            
    
            
            # grab raw txt 
            file = open(os.path.join(path, "cost_index.txt"), "r")
            text = file.read()
            file.close()
            
            # modify initial year here
            data = text.split("1978")
            
            # Remove the initial few words
            data.pop(0)
            cost_dict['1978'] = data[0:12]
            del data[0:12]
            data = data[0]
            
            # Go through text and grab data points as a function of year
            for i in range(1979,2019):
                data = data.split(str(i))
                data.pop(0)
                cost_dict[str(i)] = data[0:12]
                del data[0:12]
                data=data[0]
        
            df = pd.DataFrame(cost_dict, index = index_list)
        
            return df.applymap(remove_nonnumeric)
            
        ce_index = get_CE_index()
        
        def get_ppi_inds():
            
            """https://www.bls.gov/developers/api_signature_v2.htm"""
            # noyears is the maximum number of years you can pull from api in 1 query
            noyears = 20
            
            # the last year that you want the data from - default is this year -1
            endyear = UpdateParams.today.year -1

            # the first year that you want the data from, if not available NaN will be the value
            startyear = 1970
            
            noyear_list = [noyears] * ((endyear-startyear) // noyears) + [(endyear - startyear) % 20+1]
            year_tracker = endyear
            
            df = pd.DataFrame(columns = list(map(str,list(range(startyear,endyear+1)))))
            
            for noyears in noyear_list:
                
                headers = {'Content-type': 'application/json'}
                # please label your series as "PPI series id" : "df label name"
                series_list = \
                OrderedDict({
                        'WPU061': "Industrial Chemicals", 
                        "PCU33241033241052": "Boiler",
                        "PCU333994333994": "Furnace",
                        "PCU333414333414": "Solar Field",
                        "PCU33361133361105": "CHP",
                        "WPU10250105": "Aluminum",
                        "WPU11790105": "BatteryStorage"
                        })
                data = json.dumps({"seriesid": list(series_list.keys()), "annualaverage":"true","startyear":str(year_tracker-noyears+1), "endyear":str(year_tracker), "registrationkey":"2ad8d1d2aa574a05a389c070bee5e070"})
                p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
                json_data = json.loads(p.text)   
            
                pd_dict = {}
                
                for i in range(len(json_data["Results"]["series"])):
                    series_id = json_data["Results"]["series"][i]["seriesID"]
                    pd_dict[series_id] = [j for j in json_data["Results"]["series"][i]["data"] if j["periodName"] == "Annual"]
            
                for i in series_list.keys():
                    ser_vals = [j["value"] for j in pd_dict[i][::-1]]
                    ser_vals = [float("nan")] * (noyears - len(ser_vals)) + ser_vals
                    df.loc[series_list[i], list(map(str,list(range(year_tracker-noyears+1,year_tracker+1)))) ] = ser_vals
                    
                year_tracker -= noyears

            return df
            
        ppi_index = get_ppi_inds()
        
        comb_index = pd.concat([ce_index, ppi_index], join = "outer", sort = True)
    
        comb_index.to_csv(os.path.join(path, "cost_index_data.csv"))    
        
if __name__ == "__main__":
    UpdateParams.create_index()