# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 13:16:33 2020

@author: wxi
"""
import re
import os
from abc import ABCMeta, abstractmethod
import get_API_params as gap
import datetime
import pandas as pd
import models 
import dask.dataframe as dd
# import models - financial models for diff technology


#This abstract class defines all methods that are used externally in run file
class LCOH(metaclass=ABCMeta):
    today = datetime.datetime.now()
    path = "./calculation_data"
    @abstractmethod
    def __init__(self):

        print("Every component of the LCOH equation should be initialized.")

    @abstractmethod
    def iterLCOH(self):

        print("Updates LCOH based off updated iteration variable")

    @abstractmethod
    def calculate_LCOH(self):

        print("Use attributes to calculate LCOH")


class Greenfield(LCOH):

    def __init__(self, form):

        # process format
        f, self.temp_bin, self.load_avg = form

        self.invest_type, self.tech_type, self.iter_name = f.split(",")
        
        # should add ability to import the fuel type in the future for tool
        self.fuel_type = "NG"
        
        # assume for now peak load is same as load_avg difference below and load profile is uniform on average
        self.peak_load = self.load_avg
        self.load_8760 = [self.load_avg for i in range(0,8760)]
        
        # option to turn on just reading from csv of heat load shape 
        read_csv = False
        if (read_csv):
            df_hload = dd.read_csv(os.path.join(LCOH.path, "sample_8760_1.csv"))
            df_hload = df_hload.drop(["Unnamed: 7", "Unnamed: 8", "Unnamed: 9"], axis = 1)
            df[(df["naics"] == 325211) & (df["End_use"] == "Conventional Boiler Use") & (df["MECS_FT"] == "Natural_gas")].compute()["load_MMBtu_per_hour"].values
            hload_gb = df_hload.groupby(['naics','End_use','MECS_FT'])["load_MMBtu_per_hour"].agg(['max', 'sum'])
            # once we establish some conventional naming system for different fuels can just do regular expressions
            # to search the groupby object instead of a bunch of if statements
            if self.tech_type == "BOILER" and self.fuel_type == "NG":
                # average load since when user enters a heat load its assumed to average for 8760
                self.load_avg = hload_gb["sum"].compute().loc[(325211,'Conventional Boiler Use','Natural_gas')] / 31536000 
                self.peak_load = hload_gb["max"].compute().loc[(325211,'Conventional Boiler Use','Natural_gas')]
                mask1 = df_hload["naics"] == 325211
                mask2 = df_hload["End_use"] == "Conventional Boiler Use"
                mask3 = df_hload["MECS_FT"] == "Natural_gas"
                self.load_8760 = df_hload[mask1 & mask2 & mask3].compute()["load_MMBtu_per_hour"].values
        
        # define year for data imports
        self.year = LCOH.today.year

        # FIPS to Abbrev to State
        fips_data = pd.read_csv(os.path.join(LCOH.path, "US_FIPS_Codes.csv"),
                                usecols=['State', 'COUNTY_FIPS', 'Abbrev'])
        # get from https://www.eia.gov/totalenergy/data/monthly/#appendices spreadsheets - other industrial heat content for coal
        # NG end-use sectors heat content, commercial industrial sector heat content for petro
        #PETRO - M Btu/barrel, NG - BTU / cubic foot, COAL - Million Btu/ Short ton
        self.hv_vals = {"PETRO" : 4.641, "NG" : 1039, "COAL" : 20.739}
        # convert heating_values to the appropriate volume/mass basis and energy basis (kW)
        self.hv_vals["PETRO"] = 4.641 * 1055055.85 / 42 # / to gallon, * to kJ
        self.hv_vals["NG"] = 1039 * 1.05506 / 0.001 # / to thousand cuf * to kJ
        self.hv_vals["COAL"] = 20.739 * 1055055.85 # already in short ton, * to kJ
        # NG price unit - $/thousand cuf, Petro - $/gallon, Coal - $/ short ton

        # get county
        while True:

            try:
                self.county = str(input("Enter a FIPS code or USA: ")).strip()

                if self.county not in fips_data['COUNTY_FIPS'].values:

                    raise AssertionError("No Such County")

                break

            except AssertionError as e:

                print(e)

        # get state_name, state_abbr
        self.state_name = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'State'].values[0].strip()

        self.state_abbr = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'Abbrev'].values[0].strip()
        
        self.fuel_price, self.fuel_year = gap.UpdateParams.get_fuel_price(
                                            self.county, self.fuel_type)

        while True:

            try:
                self.p_time = int(input("Enter period of analysis (int): "))

                break

            except TypeError or ValueError:

                print("Please enter an integer.")

                continue

        self.discount_rate = 0.15
    
    # import the relevant tech to financial parameter model object
    
        self.model = models.TechFactory.create_tech(
                     self.tech_type, self.county, self.temp_bin, 
                     (self.peak_load, self.load_8760), 
                     (self.fuel_price,self.fuel_type)
                    )
        def import_param():

            def get_subsidies():
    
                """
                Returns of a dic of subsidies in the form of
                {year0: value, pinvest: lambda, time: lambda}
                year0 = fixed subsidies at year 0, %invest is a percentage of
                investment and time = time-dependent function
    
                """
    
                if self.tech_type in ['BOILER', 'FURNACE', 'KILN', 'OVEN']:
    
                    return {"year0": 0, "pinvest": 0, "time": lambda t: t*0}
    
                else:
                    """ For now to simplify, assume just the ITC tax credit """
    
                    sol_subs = pd.read_csv(os.path.join(LCOH.path, "Sol_Subs.csv"),
                                           index_col=['State'])
    
                    return {"year0": 0, "time": lambda t: t*0}
    
            self.subsidies = get_subsidies()
    
            def get_corptax():
    
                """
                Returns combined corporate tax rate - assume investment
                is always taxed at highest bracket (conservative)
    
                https://www.taxpolicycenter.org/statistics/state-corporate-income-tax-rates
                """
                corp_tax = pd.read_csv(os.path.join(LCOH.path, "corp_tax.csv"),
                                       index_col=['State'],
                                       usecols=['State', 'Total'])
    
                return corp_tax.loc[self.state_name, 'Total']
    
            self.corp_tax = get_corptax()
    
            def get_dep_value(t, no_years=5):
    
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
                    schedule = [0.2, 0.32, 0.1920, 0.1152, 0.1152, 0.0576]
    
                try:
                    return schedule[t-1]
    
                except IndexError:
    
                    return 0
    
            self.depreciation = get_dep_value
    
            def get_OM(t,OM_esc = 0.02):
                
                """ Placeholders, OM fixed/var should be obtained from model.py"""
    
                self.model.om()
                self.fc = self.load_avg * 31536000 / self.hv_vals[self.fuel_type] * self.fuel_price
    
                #  fuel price - multiply to convert the kW to total energy in a year (kW)
                #  divided by appropriate heating value 
                return (self.model.om_val + self.fc ) * (1 + OM_esc) ** t
                        
            self.OM = get_OM
    
            def get_capital():
                
                self.model.capital()
                
                return self.model.cap_val
    
            self.capital = get_capital()
    
            def get_energy_yield(t):
    
                """placeholder going to use model.py file for this"""
    
                return 10
    
            self.energy = get_energy_yield
            
        import_param()

    def __str__(self):

        return "A LCOH object of investment {} for a {} and iterating {}".\
            format(self.invest_type, self.tech_type, self.iter_name)

 
    def iterLCOH(self, iter_value = None):
        
        if iter_value is None:

            self.calculate_LCOH()

            return self.LCOH_val

        if self.iter_name == "INVESTMENT":

            self.capital = iter_value

        if self.iter_name == "FUELPRICE":

            self.fuel_price = iter_value

        self.calculate_LCOH()

        return self.LCOH_val
    
    def calculate_LCOH(self):

        """using general LCOH equation"""

        undiscounted = self.capital - self.subsidies["year0"]

        total_d_cost = 0

        t_energy_yield = 0

        for i in range(1, self.p_time+1):
            # removed 1 - self.corp_tax on OM because OM value is tax free for boiler
            d_cost = (self.OM(i) - self.capital *
                      self.depreciation(i) * self.corp_tax) / \
                      (1+self.discount_rate) ** i

            total_d_cost += d_cost
            
            # assuming constant energy yield per year

            energy_yield = self.load_avg * 31536000 / (1 + self.discount_rate) ** i

            t_energy_yield += energy_yield

        self.LCOH_val = (undiscounted + total_d_cost)/t_energy_yield
    
class LCOHFactory():
    @staticmethod
    def create_LCOH(form):
        try:
            if re.search("GREENFIELD", form[0]):
                return Greenfield(form)
# =============================================================================
#             if re.search("EXTENSION",format):
#                 return Extension(format)
#             if re.search("REPLACE",format):
#                 return Replace(format)
# =============================================================================
            raise AssertionError("No Such LCOH Equation")
        except AssertionError as e:
            print(e)
            
if __name__ == "__main__":
    pass
        