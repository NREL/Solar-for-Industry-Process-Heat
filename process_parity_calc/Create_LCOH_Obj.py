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
import numpy as np

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
        
        self.sim = False

        f, self.load_avg, self.county = form

        self.invest_type, self.tech_type = f.split(",")
        
        self.iter_name = None
        
        # should add ability to import the fuel type in the future for tool
        self.fuel_type = "NG"
        
        # assume for now peak load is same as load_avg difference below and load profile is uniform on average
        self.peak_load = self.load_avg
        self.load_8760 = [self.load_avg for i in range(0,8760)]
        
        # option to turn on just reading from csv of heat load shape 
        read_csv = False
        if (read_csv):
            df = dd.read_csv(os.path.join(LCOH.path, "sample_8760_1.csv"))
            df = df.drop(["Unnamed: 7", "Unnamed: 8", "Unnamed: 9"], axis = 1)
            df[(df["naics"] == 325211) & (df["End_use"] == "Conventional Boiler Use") & (df["MECS_FT"] == "Natural_gas")].compute()["load_MMBtu_per_hour"].values
            hload_gb = df.groupby(['naics','End_use','MECS_FT'])["load_MMBtu_per_hour"].agg(['max', 'sum'])
            # once we establish some conventional naming system for different fuels can just do regular expressions
            # to search the groupby object instead of a bunch of if statements
            if self.tech_type == "BOILER" and self.fuel_type == "NG":
                # average load since when user enters a heat load its assumed to be average for 8760
                self.load_avg = hload_gb["sum"].compute().loc[(325211,'Conventional Boiler Use','Natural_gas')] / 31536000 
                self.peak_load = hload_gb["max"].compute().loc[(325211,'Conventional Boiler Use','Natural_gas')]
                mask1 = df["naics"] == 325211
                mask2 = df["End_use"] == "Conventional Boiler Use"
                mask3 = df["MECS_FT"] == "Natural_gas"
                self.load_8760 = df[mask1 & mask2 & mask3].compute()["load_MMBtu_per_hour"].values
        
        # define year for data imports
        self.year = LCOH.today.year

        # FIPS to Abbrev to State
        fips_data = pd.read_csv(os.path.join(LCOH.path, "US_FIPS_Codes.csv"),
                                usecols=['State', 'COUNTY_FIPS', 'Abbrev'])
        

        # get state_name, state_abbr
        self.state_name = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'State'].values[0].strip()

        self.state_abbr = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'Abbrev'].values[0].strip()
        
        self.fuel_price, self.fuel_year = gap.UpdateParams.get_fuel_price(
                                            self.state_abbr, self.fuel_type)
        self.fp_range = gap.UpdateParams.get_max_fp(self.state_abbr, self.fuel_type) - self.fuel_price
        self.fuel_esc = np.array([gap.UpdateParams.get_esc(self.state_abbr, self.fuel_type) / 100])

        self.p_time = np.array([20])

        self.discount_rate = np.array([0.15])
        
        self.OM_esc = np.array([0.02])
    
        # import the relevant tech to financial parameter model object
    
        self.model = models.TechFactory.create_tech(
                     self.tech_type, 
                     (self.peak_load, self.load_8760), 
                     (self.fuel_price,self.fuel_type)
                    )
        # intialize om because all model inputs have been determined already
        self.model.om()
        
        # initialize capital
        self.model.capital()

        def import_param():

            def get_subsidies():
    
                """
                Returns of a dic of subsidies in the form of
                {year0: value, pinvest: lambda, time: lambda}
                year0 = fixed subsidies at year 0, %invest is a percentage of
                investment and time = time-dependent function
    
                """
    
                if self.tech_type in ['BOILER', 'FURNACE', 'KILN', 'OVEN']:
    
                    return {"year0": 0, "time": lambda t: t*0}
    
                else:
                    """ For now to simplify, assume just the ITC tax credit """
    
                   # sol_subs = pd.read_csv(os.path.join(LCOH.path, "Sol_Subs.csv"),
                                           #index_col=['State'])
                    
                    ITC = lambda t: 0.26 if t == 2019 else(0.22 if (t == 2020) else 0.1)
    
                    return {"year0": ITC(self.year), "time": 0}
    
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
                    schedule = [1/no_years for i in range(no_years)]
    
                try:
                    return schedule[t-1]
    
                except IndexError:
    
                    return 0
    
            self.depreciation = get_dep_value
    
            def get_OM(t):
                
                """ Placeholders, OM fixed/var should be obtained from model.py"""
                # only need to initalize during init - first calculate_LCOH
                if not self.sim:
                
                    omp = 1
                    fmp = 0
                    
                
                if self.sim:
                    
                    omp = self.rand_o
                    fmp = self.rand_f
                
                #
                self.fc = self.model.fc * (self.fuel_price + fmp) * (1 + self.fuel_esc)**t
    
                #  fuel price - multiply to convert the kW to total energy in a year (kW)
                #  divided by appropriate heating value 
                if self.tech_type == "BOILER":
      
                    return np.array([(self.model.om_val * omp * (1 + self.OM_esc) ** t + self.fc) / (1 - self.corp_tax)]).flatten()
                else:

                    return np.array([self.model.om_val * omp * (1 + self.OM_esc) ** t + self.fc]).flatten()
                
            self.OM = get_OM

            def get_capital():
                
                try:
                    return np.array([self.investment])
                
                except AttributeError:
                    pass
              
                if not self.sim:
                    
                    cmp = 1
                    
                if self.sim:
                    
                    cmp = self.rand_c
                    
                return np.array([self.model.cap_val]) * cmp

            self.capital = get_capital
            
        import_param()

    def __str__(self):

        return "A LCOH object of investment {} for a {} and iterating {}".\
            format(self.invest_type, self.tech_type, self.iter_name)

    def calculate_LCOH(self):

        """using general LCOH equation"""

        undiscounted = self.capital() - self.subsidies["year0"]* self.capital()

        total_d_cost = np.zeros(len(self.p_time))

        t_energy_yield = np.zeros(len(self.p_time))
        
        
        for ind, p_time in enumerate(self.p_time):
            
            for i in range(1, p_time+1):

                d_cost = (self.OM(i)[ind] *(1-self.corp_tax) - self.capital()[ind] *
                self.depreciation(i, self.model.dep_year) * self.corp_tax) / \
                (1+self.discount_rate[ind]) ** i
                
                total_d_cost[ind] += d_cost

                energy_yield = self.load_avg * 31536000 / (1 + self.discount_rate[ind]) ** i
                t_energy_yield[ind] += energy_yield

        # convert to cents USD/kwh   
        return (undiscounted + total_d_cost)/t_energy_yield * 3600 * 100

    def iterLCOH(self, iter_value = None):
        
        if iter_value is None:

            return self.calculate_LCOH()

        if self.iter_name == "INVESTMENT":


            self.investment = iter_value

        if self.iter_name == "FUELPRICE":

            self.fuel_price = iter_value

        return self.calculate_LCOH()
   
    def apply_dists(self, a_type, no_sims):
        
        if a_type.upper() == "MC":
            
            self.p_time = np.random.randint(25,40,size = no_sims)
            self.discount_rate = np.random.uniform(0.05,0.15, size =no_sims)
            self.OM_esc = np.random.uniform(0,0.03,size = no_sims)
            self.fuel_esc = np.random.uniform(0,0.03,size = no_sims)
            self.rand_c = np.random.triangular(0.9,1,4,size = no_sims)
            self.rand_o = np.random.triangular(0.9,1,4,size = no_sims)
            self.rand_f = np.random.uniform(0, self.fp_range, size = no_sims)
    
        if a_type.upper() == "TO":
            
            siml = int(no_sims/7)
            
            d_vals = [self.p_time[0], self.discount_rate[0], self.OM_esc[0], 
                      self.fuel_esc[0], 1, 1, 0]
            
            # could use an ordereddict if needed
# =============================================================================
#             sim =[np.random.randint(25,40,size = siml), 
#                   np.random.uniform(0.05,0.15, size = siml),
#                   np.random.uniform(0,0.03,size = siml),
#                   np.random.uniform(0,0.03,size = siml),
#                   np.random.triangular(0.9,1,4,size = siml),
#                   np.random.triangular(0.9,1,4,size = siml),
#                   np.random.uniform(0, self.fp_range, size = siml)
#                   ]
# =============================================================================

            sim =[np.linspace(25,40,num = siml), 
                  np.linspace(0.05,0.15,num = siml),
                  np.linspace(0,0.03,num = siml),
                  np.linspace(0,0.03,num = siml),
                  np.linspace(0.9,4,num = siml),
                  np.linspace(0.9,4,num = siml),
                  np.linspace(0,self.fp_range,num = siml)
                  ]
            
            vals = [np.concatenate(([val]*i*siml, sim[i], [val]*(6-i)*siml), axis=0) for i, val in enumerate(d_vals)]

            self.p_time = vals[0].astype(int)
            self.discount_rate = vals[1]
            self.OM_esc = vals[2]       
            self.fuel_esc = vals[3]
            self.rand_c = vals[4]
            self.rand_o = vals[5]   
            self.rand_f = vals[6]

                     
    def simulate(self, a_type, no_sims = 1000):
        
        if a_type.upper() == "MC":
            mc_results = pd.DataFrame(columns = [
                                                 "Lifetime",
                                                 "Nominal Discount Rate",
                                                 "O&M Escalation Rate",
                                                 "Fuel Escalation Rate",
                                                 "Capital Cost",
                                                 "Operating Cost",
                                                 "Capital Multiplier",
                                                 "Operating Multiplier",
                                                 "Fuel Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
            #initialize model capital and om attributes by running 1 calculate LCOH (default values)
            columns = list(mc_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.model.cap_val, self.model.om_val,
                         1, 1, self.fuel_price, self.calculate_LCOH()]

            mc_results = mc_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)
            
            self.sim = True
            self.apply_dists("MC", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.model.cap_val, self.model.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f, self.calculate_LCOH()]
            mc_results = mc_results.append(pd.DataFrame({a:b for a,b in zip(columns,results)}))
            #mc_results.to_csv(os.path.join(LCOH.path, "mcsim" + self.tech_type + ".csv"))
            return mc_results
        
        if a_type.upper() == "TO":
            
            to_results = pd.DataFrame(columns = [
                                                 "Lifetime",
                                                 "Nominal Discount Rate",
                                                 "O&M Escalation Rate",
                                                 "Fuel Escalation Rate",
                                                 "Capital Cost",
                                                 "Operating Cost",
                                                 "Capital Multiplier",
                                                 "Operating Multiplier",
                                                 "Fuel Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
                    
            columns = list(to_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.model.cap_val, self.model.om_val,
                         1, 1, self.fuel_price, self.calculate_LCOH()]
            
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)      
            
            self.sim = True                  
            self.apply_dists("TO", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.model.cap_val, self.model.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f, self.calculate_LCOH()]
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,results)}))    
           # to_results.to_csv(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv"))
            return to_results
            
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
    test = CLO.LCOHFactory().create_LCOH(FormatMaker().create_format())
    
