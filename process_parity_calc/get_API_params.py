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
    The intention of this class is to obtain the latest LCOH parameters that are from online databases.
    Expects inputs of NG, PETRO or, COAL
    
    NG -> industrial price
    COAL -> other industrial use price 
    Petroleum -> residual fuel oil prices by area -> wholesale/resale price by all sellers annual
    
    """
   
    path = "./calculation_data"
    
    today = datetime.datetime.now()
    
    eia_api_key = "68ea6b4094e685e32ec986a8053568d9"
    
    api = eia.API(eia_api_key)
        
    fips_data = pd.read_csv(os.path.join(path,"US_FIPS_Codes.csv"), usecols= ['State','COUNTY_FIPS','Abbrev'])
                
    def get_fuel_price(c_fips, fuel_type = "NG" , year = False):
        
        """Obtain fuel averages on the annul, state scale from the EIA database."""
        
        if(not year):
            
            year = UpdateParams.today.year
        
        try:
            
            state_abbr = UpdateParams.fips_data.loc[UpdateParams.fips_data['COUNTY_FIPS'] \
                                                == str(c_fips), 'Abbrev'].values[0].strip()
        except KeyError: 
            
            print("Enter a valid county FIPS code")   
            
        if fuel_type.upper() == "NG":
            
            series_ID = "NG.N3035" + state_abbr + "3.A"
            
            series_USA = "NG.N3035US3.A"
        
        elif fuel_type.upper() == "COAL":
            
            series_ID = "COAL.COST." + state_abbr + "-10.A"
            
            series_USA = "COAL.COST.US-10.A"
            
        elif fuel_type.upper() == "PETRO":
            
            series_ID = "PET.EMA_EPPR_PWA_S" + state_abbr + "_DPG.A" 
            
            series_USA = "PET.EMA_EPPR_PWA_NUS_DPG.A"
            
        else:
            raise AssertionError ("Please input a valid fuel_type")
        
        
    
        fuel_series_USA = UpdateParams.api.data_by_series(series = series_USA)
        
        dict_key_USA = list(fuel_series_USA.keys())[0]
        
        #find latest USA value
        
        i = 0
        
        while True:
            
            try:
                    
                fuel_series_USA[dict_key_USA][str(year-i) + "  "] / 1.0
                
            except:
                
                i += 1
        
        #Check if state-level available, if not return USA price
        
        try:
            
            #only line below will raise key error
            
            fuel_series = UpdateParams.api.data_by_series(series = series_ID)
            
            dict_key = list(fuel_series.keys())[0]
            
            #if fuel price in state is empty return national price 
            
            if all(v is None for v in list(fuel_series[dict_key].values())):
                
                return (fuel_series_USA[dict_key_USA][str(year-i) + "  "] , year-i)
            
        except KeyError:
            
            return (fuel_series_USA[dict_key_USA][str(year-i) + "  "] , year-i)
        
        j = 0
        
        #find latest year for state 
        
        while True:
            
            try:
                    
                fuel_series[dict_key][str(year-j) + "  "] / 1.0
                
            except:
                
                j += 1
        
        #find latest USA value, if this is 2 years more recent than the state
        #value return the USA value
        
        if (year-i) - (year-j) >=2:
            
            return (fuel_series_USA[dict_key_USA][str(year-i) + "  "] , year-i)
    
        
        return (fuel_series[dict_key][str(year-j) + "  "] , year-j)
        
       
        
