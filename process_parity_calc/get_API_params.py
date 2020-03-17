# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:37:30 2020

@author: wxi
"""

import datetime
import pandas as pd
import os
import eia

class UpdateParams:
    
    """The intention of this class is to obtain the latest LCOH parameters that are from online databases."""
   
    path = "./calculation_data"
    
    today = datetime.datetime.now()
    
    eia_api_key = "68ea6b4094e685e32ec986a8053568d9"
    
    api = eia.API(eia_api_key)
        
    fips_data = pd.read_csv(os.path.join(path,"US_FIPS_Codes.csv"))
    
    def get_ngprice(self, c_fips, year = False):
        
        """Obtain natural gas price averages on the annul, state scale from the EIA database."""
        
        if(not year):
            year = UpdateParams.today.year
        
        state_abbr = UpdateParams.fips_data.loc[UpdateParams.fips_data['COUNTY_FIPS'] \
                                                == c_fips, 'Abbrev'].values[0].strip()
        
        series_ID = "NG.N3035" + state_abbr + "3.A"
        
        ng_series = UpdateParams.api.data_by_series(series = series_ID)  
     
        dict_key = list(ng_series.keys())[0]
        
        i = 0
     
        while True:
    
            try:
                
                ng_series[dict_key][str(year-i) + "  "]
                
                #catches none-types below
                
                ng_series[dict_key][str(year-i) + "  "] / 1
                    
            except:

                i += 1 
                
            else:

                return ng_series[dict_key][str(year-i) + "  "]
        
    def get_coalprice(self, c_fips, year = False):
        
        """Obtain fuel coal averages on the annul, state scale from the EIA database."""
        
        if(not year):
            year = UpdateParams.today.year
    
        state_abbr = UpdateParams.fips_data.loc[UpdateParams.fips_data['COUNTY_FIPS'] \
                                                == c_fips, 'Abbrev'].values[0].strip()
        
        series_ID = "COAL.COST." + state_abbr + "-10.A"
        
        series_USA = "COAL.COST.US-10.A"
        
        #this try except else is used to catch states that don't have coal prices
        
        try:
        
            coal_series = UpdateParams.api.data_by_series(series = series_ID)  
        
        except:
            
            coal_series = UpdateParams.api.data_by_series(series = series_USA)
            
            dict_key = list(coal_series.keys())[0]
            
        else:
            
            dict_key = list(coal_series.keys())[0]
        
        i = 0
     
        while True:
    
            try:
                
                coal_series[dict_key][str(year-i) + "  "]
                
                #catches none-types below
                
                coal_series[dict_key][str(year-i) + "  "] / 1
                    
            except:

                i += 1 
                
            else:

                return coal_series[dict_key][str(year-i) + "  "]
        
