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


class UpdateParams:

    """
    The intention of this class is to obtain the latest LCOH parameters that
    are from online databases.
    Expects inputs of NG, PETRO or, COAL

    NG -> industrial price ($ per thousand cubic feet)
    COAL -> other industrial use price ($ per short ton)
    Petroleum -> residual fuel oil prices by area -> wholesale/resale price by
    all sellers annual ($ per gallon)

    """

    path = "./calculation_data"

    today = datetime.datetime.now()
    
    eia_api_key = "68ea6b4094e685e32ec986a8053568d9"
    
    api = eia.API(eia_api_key)
          
    eerc_esc = pd.read_csv(os.path.join(path, "EERC_Fuel_Esc.csv"), index_col = ["State"])

    def get_fuel_price(state_abbr, fuel_type="NG", year=False):

        """Obtain fuel avgs on the annul, state scale from the EIA database."""

        if(not year):

            year = UpdateParams.today.year

        if fuel_type.upper() == "NG":

            series_ID = "NG.N3035" + state_abbr + "3.A"

            series_USA = "NG.RNGWHHD.A"

        elif fuel_type.upper() == "COAL":

            series_ID = "COAL.COST." + state_abbr + "-10.A"

            series_USA = "COAL.COST.US-10.A"

        elif fuel_type.upper() == "PETRO":

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
                fuel_series_USA[dict_key_USA][str(year-i) + "  "] / 1.0

                break

            except:
                
                i += 1

        # Check if state-level available, if not return USA price
        try:
            fuel_series = UpdateParams.api.data_by_series(series=series_ID)

            dict_key = list(fuel_series.keys())[0]

            # if fuel price in state is empty return national price
            if all(v is None for v in list(fuel_series[dict_key].values())):
                
                return (fuel_series_USA[dict_key_USA][str(year-i) + "  "], year-i)
                
        except KeyError:
                
            return (fuel_series_USA[dict_key_USA][str(year-i) + "  "], year-i)

        j = 0

        # find latest year for state
        while True:

            try:
                fuel_series[dict_key][str(year-j) + "  "] / 1.0

                break

            except:

                j += 1
                
        # return USA value if 2 years more recent vs state
        if ((year-i) - (year-j) >= 2) | (fuel_series[dict_key][str(year-j) + "  "] >= fuel_series_USA[dict_key_USA][str(year-i) + "  "]):
                
            return (fuel_series_USA[dict_key_USA][str(year-i) + "  "], year-i)

        return (fuel_series[dict_key][str(year-j) + "  "], year-j)
    
    def get_esc(state_abbr, fuel_type="NG"):
    
        """Grabs fuel esc from EERC"""
        
        temp_dict = {"NG": "Natural Gas", "COAL" : "Coal", "PETRO": "Residual"}
        
        return UpdateParams.eerc_esc.loc[state_abbr, temp_dict[fuel_type]]
        
      
        
# =============================================================================
#     def get_fuel_esc():
#         
#         ''' creates a csv file of fuel_esc'''
#         
#         ng_ser = UpdateParams.api.data_by_series("NG.N3035US3.A")
#         key_ng = list(ng_ser.keys())[0]
#         
#         coal_ser = UpdateParams.api.data_by_series("COAL.COST.US-10.A")
#         key_coal = list(coal_ser.keys())[0]
#         
#         petro_ser = UpdateParams.api.data_by_series("PET.EMA_EPPR_PWG_NUS_DPG.A")
#         key_petro = list(petro_ser.keys())[0]
#         
#         fuel_esc_df = pd.DataFrame()
#         
#         j = 0
#         
#         # take first 15 years 
#         for i in [petro_ser[key_petro], coal_ser[key_coal], ng_ser[key_ng]]:
#             
#             prices = [v for k,v in i.items()]
#             
#             esc = [(a / b) ** 0.2 - 1 for a, b in zip(prices[0:11], prices[4:15])]
#             
#             fuel_esc_df[str(j)] = esc
#             
#             j += 1
#         
#         fuel_esc_df.rename(columns={"0": "PETRO", "1": "COAL", "2" : "NG"}, inplace = True)
#         fuel_esc_df.to_csv(os.path.join(UpdateParams.path,"fuel_esc_data.csv"))
# =============================================================================
        
if __name__ == "__main__":
    # generate the range of inflation values for yearly escalation
    a = UpdateParams
    print(a.get_fuel_price("CA"))
