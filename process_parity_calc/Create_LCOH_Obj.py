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
import numpy as np
import json

#This abstract class defines all methods that are used externally in run file
class LCOH(metaclass=ABCMeta):
    today = datetime.datetime.now()
    path = "./calculation_data"
    corp_tax = pd.read_csv(os.path.join(path, "corp_tax.csv"),
                        index_col=['State'],
                        usecols=['State', 'Total'])

    fips_data = pd.read_csv(os.path.join(path, "US_FIPS_Codes.csv"),
                            usecols=['State', 'COUNTY_FIPS', 'Abbrev']) 

    year = today.year
    @abstractmethod
    def __init__(self, form):
        # format processing
        self.sim = False
        self.iter_name = None
        read_csv = True
        
        self.invest_type, self.tech_type, self.load_avg, self.county = form
        self.peak_load = self.load_avg
        self.load_8760 = [self.load_avg for i in range(0,8760)]
        self.fuel_type = "NG" # should add ability to import the fuel type in the future for tool
        
        # Assume in kW
        if (read_csv):   
            df = pd.read_csv(os.path.join(LCOH.path, "county_loads_8760.csv"))
            self.fe_load_8760 = df.loc[df["FIPS"] == self.county]["eload"]
            self.load_8760 = df.loc[df["FIPS"] == self.county]["load"]
            self.load_avg = np.mean(self.load_8760)
            self.peak_load = max(self.load_8760)

        self.state_name = LCOH.fips_data.loc[LCOH.fips_data['COUNTY_FIPS'] == self.county,
                                        'State'].values[0].strip()

        self.state_abbr = LCOH.fips_data.loc[LCOH.fips_data['COUNTY_FIPS'] == self.county,
                                        'Abbrev'].values[0].strip()
        
        self.fuel_price, self.fuel_year = gap.UpdateParams.get_fuel_price(
                                            self.state_abbr, self.fuel_type)
        with open('./calculation_data/elec_curve.json') as f:
            elec_curves = json.load(f)
            
        self.pd_curve = [elec_curves[self.state_abbr]]

        self.fp_range = gap.UpdateParams.get_max_fp(self.state_abbr, self.fuel_type) - self.fuel_price

        self.fuel_esc = np.array([gap.UpdateParams.get_esc(self.state_abbr, self.fuel_type) / 100])
        
        self.p_time = np.array([20])

        self.discount_rate = np.array([0.15])
        
        self.OM_esc = np.array([0.02])
        
        self.elec_esc = np.array([0.0125])
        
    @abstractmethod
    def calculate_LCOH(self):

        print("Use attributes to calculate LCOH")

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
            return schedule[t-1]

        except IndexError:

            return 0     

    @classmethod
    def get_subsidies(cls, tech_type, state):

        """
        Dsire database filters: financial incentive, excluding sir, 
        eligible sector: industrial, state/territory, 
        technology: all solar then manually check
        """

        if tech_type in ['BOILER', 'FURNACE', 'KILN', 'OVEN']:

            return {"year0": 0, "time": lambda t: t*0}

        else:
            """ For now to simplify, assume just the ITC tax credit """


            ITC = lambda t: 0.26 if t == 2019 else(0.22 if (t == 2020) else 0.1)

            return {"year0": ITC(LCOH.year), "time": 0}
    
    @classmethod  
    def get_corptax(cls, state_name):
    
        """
        Returns combined corporate tax rate - assume investment
        is always taxed at highest bracket (conservative)
    
        https://www.taxpolicycenter.org/statistics/state-corporate-income-tax-rates
        """
    
        return LCOH.corp_tax.loc[state_name, 'Total']  

    def __str__(self):
        return "A LCOH object of investment {} for a {} and iterating {}".\
            format(self.invest_type, self.tech_type, self.iter_name)

    def iterLCOH(self, iter_value = None):
        
        if iter_value is None:

            return self.calculate_LCOH()

        if self.iter_name == "INVESTMENT":

            self.investment = iter_value

        if self.iter_name == "FUELPRICE":

            self.fuel_price = iter_value

        return self.calculate_LCOH()
        print("Updates LCOH based off updated iteration variable")
        
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

            sim =[np.linspace(20,40,num = siml), 
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


class Greenfield(LCOH):

    def __init__(self, form):

        # Control variables
        LCOH.__init__(self, form)  
    
        # Model Import
        self.model = models.TechFactory.create_tech(
                     self.tech_type, self.county,
                     (self.peak_load, self.load_8760), 
                     (self.fuel_price,self.fuel_type)
                    )
        try:
            if sum(self.model.load_remain) > 0.3 * sum(self.load_8760):
                print("Should Reconsider Greenfield Investment")
            else:
                #for now assume no storage
                self.model.gf_size()
        except AttributeError:
            pass

        self.model.om()
        self.model.capital()

        def import_param():

            self.corp_tax = LCOH.get_corptax(self.state_name)
            
            self.subsidies = LCOH.get_subsidies(self.tech_type, self.state_abbr)
    
            self.depreciation = LCOH.get_dep_value
    
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
                def get_elec_cost():
                    
                    diff = self.model.elec_gen - self.fe_load_8760
                    mit_cost = sum(self.model.elec_gen[diff <= 0] * self.pd_curve[0][diff <=0]) \
                                + sum(self.fe_load_8760[diff >0] *self.pd_curve[0][diff >0])
                    sell_cost = sum(diff[diff > 0] * self.pd_curve[1][diff > 0])
                    
                    return mit_cost + sell_cost
                    
                self.ec = get_elec_cost() *(1 + self.elec_esc)**t
                self.em_costs = self.model.em_costs

                return np.array([self.model.om_val * omp * (1 + self.OM_esc) ** t + self.fc + self.ec + self.em_costs]).flatten()
                
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

                energy_yield = self.load_avg * 8760 / (1 + self.discount_rate[ind]) ** i
                t_energy_yield[ind] += energy_yield * (1-self.corp_tax)
                
            total_d_cost[ind] += self.model.decomm / (1+self.discount_rate[ind]) ** p_time
        # convert to cents USD/kwh  
        return (undiscounted + total_d_cost)/t_energy_yield * 100

                     
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
            
            if not os.path.exists(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv")):
                to_results.to_csv(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv"))
            return to_results
        
class Replace(LCOH):

    def __init__(self, form):
        
        # assert tech type to be solar only
        LCOH.__init__(self, form) 
        # add assertion
        self.lr = 0.5
        sload = [(self.lr) * load for load in self.load_8760]
        # import the relevant tech to financial parameter model object
        # default model
        self.smodel = models.TechFactory.create_tech(
                     self.tech_type, self.county,
                     (self.peak_load * self.lr, sload), 
                     (self.fuel_price,self.fuel_type)
                    )
        
        self.dmodel = models.TechFactory.create_tech(
                     "CHP", self.county,
                     (self.peak_load, self.smodel.load_remain), 
                     (self.fuel_price,self.fuel_type)
                    )

        # intialize om because all model inputs have been determined already
        self.dmodel.om()
        self.smodel.om()
        
        # initialize capital
        self.dmodel.capital()
        self.smodel.capital()
        
        self.models = [self.dmodel, self.smodel]

        def import_param():

    
            self.subsidies = np.array([LCOH.get_subsidies("BOILER"), LCOH.get_subsidies(self.tech_type)])

            self.corp_tax = LCOH.get_corptax(self.state_name)
    
            self.depreciation = LCOH.get_dep_value
    
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
                self.fc = (self.smodel.fc + self.dmodel.fc) * (self.fuel_price + fmp) * (1 + self.fuel_esc)**t
                self.ec = sum((self.smodel.elec_gen + self.dmodel.elec_gen) * self.pd_curve) *(1 + self.elec_esc)**t
    
                #  fuel price - multiply to convert the kW to total energy in a year (kW)
                #  divided by appropriate heating value 
   
                return np.array([(self.smodel.om_val + self.dmodel.om_val) * omp * (1 + self.OM_esc) ** t + self.fc + self.ec]).flatten()
                
            self.OM = get_OM

            def get_capital():
                
                try:
                    return np.array([self.investment])
                
                except AttributeError:
                    pass
              
                if not self.sim:
                    
                    cmp = np.array([1,1]).reshape(-1,1)
                    
                if self.sim:
                    
                    cmp = np.array([np.ones(len(self.rand_c)), self.rand_c])
                    
                return np.multiply(np.array([self.dmodel.cap_val, self.smodel.cap_val]).reshape(-1,1), cmp)

            self.capital = get_capital
            
        import_param()


    def calculate_LCOH(self):

        """using general LCOH equation"""
        
        cap_subs = np.array([x["year0"] for x in self.subsidies]).reshape(-1,1)
        undiscounted = self.capital()[1] - np.sum(np.multiply(cap_subs, self.capital()),0)
        
        total_d_cost = np.zeros(len(self.p_time))

        t_energy_yield = np.zeros(len(self.p_time))
        
        for ind, p_time in enumerate(self.p_time):
            for i in range(1, p_time+1):
                depreciation = np.array([self.depreciation(i, self.dmodel.dep_year), self.depreciation(i, self.smodel.dep_year)]).reshape(-1,1)
                d_cost = (self.OM(i)[ind] *(1+self.corp_tax) - np.sum(np.multiply(depreciation, self.capital()),0)[ind] * self.corp_tax) / \
                (1+self.discount_rate[ind]) ** i
                total_d_cost[ind] += d_cost

                energy_yield = self.load_avg * 31536000 / (1 + self.discount_rate[ind]) ** i
                t_energy_yield[ind] += energy_yield
        # convert to cents USD/kwh  
        return (undiscounted + total_d_cost)/t_energy_yield * 3600 * 100

                     
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
                         self.fuel_esc, self.smodel.cap_val, self.smodel.om_val+self.dmodel.om_val,
                         1, 1, self.fuel_price, self.calculate_LCOH()]
    

            mc_results = mc_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)
            
            self.sim = True
            self.apply_dists("MC", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.smodel.cap_val, self.dmodel.om_val + self.smodel.om_val,
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
                         self.fuel_esc, self.smodel.cap_val, self.smodel.om_val + self.dmodel.om_val,
                         1, 1, self.fuel_price, self.calculate_LCOH()]
            
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)      
            
            self.sim = True                  
            self.apply_dists("TO", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.smodel.cap_val, self.smodel.om_val + self.dmodel.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f, self.calculate_LCOH()]
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,results)})) 
            
            if not os.path.exists(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv")):
                to_results.to_csv(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv"))
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
# =============================================================================
            if re.search("REPLACE", form[0]):
                return Replace(form)
            raise AssertionError("No Such LCOH Equation")
        except AssertionError as e:
            print(e)
            
if __name__ == "__main__":
    
    load = 30000
    test1 = LCOHFactory().create_LCOH(('GREENFIELD', 'PTC', load, '6085'))
# =============================================================================
#     print(test1.calculate_LCOH())
#     print(test1.em_costs)
#     print(test1.em_costs/test1.OM(0) * 100) 
# =============================================================================
# =============================================================================
#     print(test.model.em_costs)
#     print(test.model.em_costs/test.model.om_val * 100) 
#     print(test.model.em_t)
# =============================================================================

# =============================================================================
#     test = LCOHFactory().create_LCOH(('GREENFIELD', 'PVHP', load, '6085'))
#     print(test.calculate_LCOH())
#     print(test.model.om_val)
# =============================================================================
