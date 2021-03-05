# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 17:33:08 2020

@author: wxi
"""
import numpy as np
import datetime
import pandas as pd
import os
import json


class ParamMethods:
    today = datetime.datetime.now()
    year = today.year
    path = "./calculation_data"
    corp_tax = pd.read_csv(os.path.join(path, "corp_tax.csv"), index_col=['State'], usecols=['State', 'Total'])
    fips_data = pd.read_csv(os.path.join(path, "US_FIPS_Codes.csv"), usecols=['State', 'COUNTY_FIPS', 'Abbrev']) 
    land_price = pd.read_csv(os.path.join(path, "landprices.csv"), index_col = 0)
    agland_price = pd.read_csv(os.path.join(path, "aglandprices.csv"), index_col = 0)
    #

    mset2 = [43756, 746]
    config = {"fuel" : "NG", 
          "ptime": np.array([20]),
          "discount": {"BOILER": 0.0658, "CHP": 0.0658, "PVHP": 0.0658, "PTC" : 0.0658, 
                       "PTCTES" : 0.0658, "DSGLF" : 0.0658, "SWH": 0.0658, "PVEB" : 0.0658,
                       "PVRH" : 0.0723, "FURNACE": 0.0723, "EBOILER": 0.0658},
          "naics": 312120,
          "emp_size": "n1000",
          "op_hour": "avg",
          "omesc": np.array([0.02]),
          "elecesc": np.array([0.0125]),
          "sf": 0.01, 
          "comb": "BOILER",
          "td": 0.25,
          "srange": 0.5,
          #mode in sf or su or default
          "mode" : "su",
          #fuel usage reduction is in kw
          "measurements": {"state" : False,
                          "312120": {"BOILER": mset2, "CHP" : mset2, "PTC" : mset2, 
                                     "PTCTES" : mset2, "DSGLF" : mset2, "PVEB" : mset2,
                                     "EBOILER": mset2, "FUEL": 264.9 },
                          "331524": 
                              {"PVRH": [0], "FURNACE": [0], "FUEL": 0}},
          "permit": {"year0": {"CA": [2724.67, 7356.83, 2676.30], "FL": [5000,250,1500], "VA": [11192], "CO": [4649.16], "OH": [100,100]},
                     "annual": {"CA": [1135.61, 457.69], "FL": [300], "VA": [0], "CO": [216, 216.24], "OH": [170]}}
                            
        }
    with open('./calculation_data/elec_curve.json') as f:
        elec_curves = json.load(f)
    with open('./calculation_data/allelecrates.json') as f:   
        elec_rates = json.load(f)

    @classmethod
    def get_lp(cls,county, ag = False):
        if ag:
            state_name = ParamMethods.fips_data.loc[ParamMethods.fips_data['COUNTY_FIPS'] == county,'State'].values[0].strip()
            return float(ParamMethods.agland_price.loc[state_name.upper(), "Value"].min().replace(',', ''))
        return ParamMethods.land_price.loc[ParamMethods.land_price["County"] == int(county), "Price Per Acre"].max()        
    @classmethod
    def get_elec_curve(cls,county, state_abbr):
        # api for open ei: 6owfvbw5T75IsLK8LJU4vjVnXBMfB1sa38HvhRwu
        if county == str(6037):
            d = datetime.datetime.today()
            year = int(d.strftime("%Y")) - 1
            date1 = str(year) + '-01-01'
            date2 = str(year+1) + '-01-01'
            hourlystamp = pd.date_range(date1, date2, freq = "1H").tolist()[:8760]
            
            def get_la(hour,day,month):
                if month in [6,7,8]:
                    if day in [0,1,2,3,4]:
                        if hour in [13,14,15,16]:
                            return 2.25
                        elif hour in [10,11,12,17,18,19]:
                            return 1.10
                        else:
                            return 0.5
                    else:
                        return 0.5
                else:
                    if day in [0,1,2,3,4]:
                        if hour in [13,14,15,16]:
                            return 1.3
                        elif hour in [10,11,12,17,18,19]:
                            return 0.9
                        else:
                            return 0.5
                    else:
                        return 0.5      

            return [np.array(ParamMethods.elec_rates[county]["touenergy"]), 
                    np.array([get_la(i.hour, i.dayofweek, i.month) * 0.13 for i in hourlystamp])]
        
        return [np.array(ParamMethods.elec_rates[county]["touenergy"]), np.array(ParamMethods.elec_curves[state_abbr])/1000]
    @classmethod 
    def get_demand_struc(cls,county):
        try:
            return ParamMethods.elec_rates[county]["flatdemand"]
        except KeyError:
            return ParamMethods.elec_rates[county]["toudemand"]
        
    @classmethod 
    def get_state_names(cls, county):
        state_name = ParamMethods.fips_data.loc[ParamMethods.fips_data['COUNTY_FIPS'] == county,'State'].values[0].strip()
        state_abbr = ParamMethods.fips_data.loc[ParamMethods.fips_data['COUNTY_FIPS'] == county,'Abbrev'].values[0].strip()
        return (state_name, state_abbr)
    
    @classmethod
    def get_dep_value(cls, t, no_years=5):

        """
        Please input number of years of schedule.
        Check https://www.irs.gov/pub/irs-pdf/p946.pdf

        """
        if (no_years == 3):
            schedule = [0.3333, 0.4445, 0.1481, 0.0741]
        elif (no_years == 5):
            schedule = [0.2, 0.32, 0.1920, 0.1152, 0.1152, 0.0576]
        elif (no_years == 7):
            schedule = [0.1429, 0.2449, 0.1749, 0.1249, 0.0893, 0.0892,
                        0.0893, 0.0446]
        elif (no_years == 10):
            schedule = [0.1, 0.18, 0.144, 0.1152, 0.0922, 0.0737, 0.0655,
                        0.0655, 0.0656, 0.0655, 0.0328]
        else:
            schedule = [1/no_years for i in range(no_years)]

        try:
            return schedule[t-1]#/1.012**t

        except IndexError:

            return 0     

    @classmethod
    def get_subsidies(cls, tech_type, county, state):

        """
        Dsire database filters: financial incentive, excluding sir, 
        eligible sector: industrial, state/territory, 
        technology: all solar then manually check
        """

        if tech_type in ['BOILER', 'FURNACE', 'CHP', "EBOILER"]:
            """ For now to simplify, assume just the ITC tax credit """
            #% capital
            p_cap = 0
            #fixed capital
            cap = 0
            #function of theoretical system size
            size = 0
            # function of kwh production
            var_size = 0
            
            return {"p_cap": p_cap, "cap": cap ,"size": size, "var_size": var_size}

        else:
            #% capital
            p_cap = 0
            #fixed capital
            cap = 0
            #function of theoretical system size (kw)
            size = 0
            # function of kwh production
            var_size = 0
            
            # ITC subsidy
            ITC = lambda t: 0.26 if t == 2019 else(0.22 if (t == 2020) else 0.1)
            p_cap += ITC(ParamMethods.year)
            
            # Specific subsidies below
            if tech_type in ["PVEB", "PVRH"]:
                if state.upper() == "NY":
                    size += 13.4/100 * 8760 * 0.09
                    # 50000 for storage + efficiency to implement later
                if state.upper() == "MO":
                    # assume apply statewide - 250$/kW
                    size += 0.25 * 1000
            if state.upper() == "TX":
                #remove 100% of system cost from taxable capital under franchise tax
                #franchise tax =0.75% so u save 0.75% * system cost
                p_cap += 0.75/100
            #pfm per kwh
            if state.upper() == "MN":
                # assume 50000 rebate given cause system will always > 300 kW
                cap += 50000
            return {"p_cap": p_cap, "cap": cap ,"size": size, "var_size": var_size}    
        
    @classmethod  
    def get_corptax(cls, state_name):
    
        """
        Returns combined corporate tax rate - assume investment
        is always taxed at highest bracket (conservative)
    
        https://www.taxpolicycenter.org/statistics/state-corporate-income-tax-rates
        """
    
        return ParamMethods.corp_tax.loc[state_name, 'Total']  
