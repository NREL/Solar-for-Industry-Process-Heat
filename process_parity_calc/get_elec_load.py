# -*- coding: utf-8 -*-
"""
Created on Mon Aug 17 12:05:35 2020

@author: wxi
"""

from datetime import datetime
import pandas as pd
import numpy as np

class get_load_8760:
    
    def __init__(self, filepath, naics, emp_size, end_use, ft, county, steam = True):
        '''
        naics - 6 digit naics code
        emp_size: ['n100_249', 'n1_49', 'n500_999', 'n50_99', 'n250_499', 'n1000']
        end_use: ['CHP and/or Cogeneration Process', 'Conventional Boiler Use', 'Process Heating']
        mecs_ft: ['Diesel', 'LPG_NGL', 'Natural_gas', 'Other', 'Residual_fuel_oil','Coal', 'Coke_and_breeze']
        fluid: working on temp >100C for steam (assumption) < 100C for not steam
        '''
        self.naics = naics
        self.emp_size = emp_size
        self.end_use = end_use
        self.ft = ft
        self.county = int(county)
        self.fluid = steam
        self.hfilepath = filepath
        self.efilepath = "./calculation_data/elec_loads.csv"

    def get_shape(self):
        """ 
        outputs the 8760 file for the current year (non leap year) Assume last year
        dayofweek - > monday = 0
        boiler shapes 
        """
        # heat load shapes
        hloadshapes = pd.read_csv(self.hfilepath, compression='gzip')   
        mask1 = hloadshapes["naics"] == self.naics
        mask2 = hloadshapes["Emp_Size"] == self.emp_size
        hloadshapes = hloadshapes[mask1 & mask2].reset_index(drop = True)
        
        #elec load shapes
        eloadshapes = pd.read_csv(self.efilepath, compression = "gzip")
        mask1 = eloadshapes["Emp_Size"] == self.emp_size
        mask2 = eloadshapes["naics"] == self.naics
        eloadshapes = eloadshapes[mask1 & mask2]
        
        d = datetime.today()
        year = int(d.strftime("%Y")) - 1
        date1 = str(year) + '-01-01'
        date2 = str(year+1) + '-01-01'
        
        hourlystamp = pd.date_range(date1, date2, freq = "1H").tolist()[:8760]
        
        def get_load_frac(df, hour,day,month, ophours):
            hmask = df['hour'] == hour
            dmask = df['dayofweek'] == day
            mmask = df['month'] == month
            return df[hmask & dmask & mmask][ophours].values[0]
    
        self.hfrac_8760 = [get_load_frac(hloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours") for i in hourlystamp]
        self.hfrac_8760_l = [get_load_frac(hloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours_low") for i in hourlystamp]
        self.hfrac_8760_h = [get_load_frac(hloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours_high") for i in hourlystamp]       

        self.efrac_8760 = [get_load_frac(eloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours") for i in hourlystamp]
        self.efrac_8760_l = [get_load_frac(eloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours_low") for i in hourlystamp]
        self.efrac_8760_h = [get_load_frac(eloadshapes, i.hour, i.dayofweek, i.month, "Weekly_op_hours_high") for i in hourlystamp]       

    
    def get_annual_loads(self, hload = False, eload = False):
        """
        if heat -> heat, false = electricity
        heat load is more specific since specific end- use
        electricity needs facility level so not as specific
        """
        # first part get peak kwh 
        if hload:
            self.hload = hload
        else:
            hdemand = pd.read_parquet('./calculation_data/mfg_eu_temps_20200728_0810.parquet.gzip')
            hdemand["MMBtu"] /= hdemand["est_count"]
            mask1 = hdemand["naics"] == self.naics
            mask2 = hdemand["End_use"] == self.end_use
            mask3 = hdemand["MECS_FT"] == self.ft
            mask4 = hdemand["Emp_Size"] == self.emp_size
            if self.fluid:
                mask5 = hdemand["Temp_C"] > 99
            mask6 = hdemand["COUNTY_FIPS"] == self.county
            filt = [mask1 & mask2 & mask3 & mask4 & mask5 & mask6]
            self.hload = hdemand[filt].max()["MMBtu"] * 293.07
            self.hcounty = hdemand[filt].max()["COUNTY_FIPS"]
            if self.hload.empty:
                raise AssertionError("Filter Conditions Failed")      
        
        if eload:
            self.eload = eload
        else:
              
            edemand = pd.read_csv("./calculation_data/net_elec.csv")
            edemand.drop(["Unnamed: 0" , "MECS_NAICS_dummies"], inplace = True, axis = 1)
            edemand["naics"] = edemand["naics"].astype(int)
            edemand["fips_matching"] = edemand["fips_matching"].astype(int)
            edemand["MMBtu_TOTAL"] /= edemand["est_count"]
            
            emask1 = edemand["naics"] == self.naics
            emask2 = edemand["Emp_Size"] == self.emp_size
            emask3 = edemand["fips_matching"] == self.county
            
            self.eload = edemand[emask1 & emask2 & emask3].max()["MMBtu_TOTAL"] * 293.07
            self.ecounty = edemand[emask1 & emask2 & emask3].max()["fips_matching"] 
        try:
            if self.ecounty != self.hcounty:
                raise AssertionError("Counties do not match")
        except AttributeError:
            pass

    def get_8760_loads(self):
        '''
        saves 8760 load to csv file
        convert to kwh
        '''
        
        self.h_8760 = self.hload/sum(self.hfrac_8760) * np.array(self.hfrac_8760)
        self.h_8760_l = self.hload/sum(self.hfrac_8760_l) * np.array(self.hfrac_8760_l)
        self.h_8760_h = self.hload/sum(self.hfrac_8760_h) * np.array(self.hfrac_8760_h)

        if round(sum(self.h_8760)) != round(self.hload):
            raise AssertionError("Heat loads don't add up")
            
        self.e_8760 = self.eload/sum(self.efrac_8760) * np.array(self.efrac_8760)
        self.e_8760_l = self.eload/sum(self.efrac_8760_l) * np.array(self.efrac_8760_l)
        self.e_8760_h = self.eload/sum(self.efrac_8760_h) * np.array(self.efrac_8760_h)

        if round(sum(self.e_8760)) != round(self.eload):
            raise AssertionError("Elec loads don't add up")
        df = pd.DataFrame()
        df["eload"] = self.e_8760
        df["eload_h"] = self.e_8760_h
        df["eload_l"] = self.e_8760_l
        df["hload"] = self.h_8760
        df["hload_h"] = self.h_8760_h
        df["hload_l"] = self.h_8760_l
        
        df.to_csv("./calculation_data/loads_8760_" + self.emp_size + "_" + str(self.naics) + ".csv")

if __name__ == "__main__":
    prh = "./calculation_data/all_load_shapes_process_heat_20200728.gzip"
    boil = "./calculation_data/all_load_shapes_boiler_20200728.gzip"
    test = get_load_8760(boil, 312120, 'n1000','Conventional Boiler Use', 'Natural_gas', '55063')
    test.get_shape()
    # in kwh
    test.get_annual_loads(11800555.56  , 329478.282149*293.07)
    test.get_8760_loads()
    