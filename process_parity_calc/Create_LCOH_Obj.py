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
from lcoh_config import ParamMethods as pm
from bisection_method import bisection
from model_config import ModelParams as mp
#This abstract class defines all methods that are used externally in run file
class LCOH(metaclass=ABCMeta):
    today = datetime.datetime.now()
    year = today.year
    path = "./calculation_data"

    @abstractmethod
    def __init__(self, form):
        # format processing
        self.sim = False
        self.measurements = pm.config["measurements"]["state"]
        self.iter_name = None
        self.form = form
        self.invest_type, self.tech_type, self.mult, self.county = form

        self.fuel_type = pm.config["fuel"]
        self.hv_vals = mp.hv_vals[self.fuel_type]
        
        path = "loads_8760_" + pm.config["emp_size"] + "_" + str(pm.config["naics"]) + ".csv"
        df = pd.read_csv(os.path.join(LCOH.path, path))
        if pm.config["op_hour"] == "high":
            elec = "eload_h"
            heat = "hload_h"
        elif pm.config["op_hour"] == "low":
            elec = "eload_l"
            heat = "eload_l"
        elif pm.config["op_hour"] == "avg":
            elec = "eload"
            heat = "hload"
        else:
            raise AssertionError("op_hour not specified")

        self.fe_load_8760 = np.array(df[elec])
        self.load_8760 = np.array(df[heat])
        
# =============================================================================
#         #only for testing
#         dftheo = pd.read_csv(os.path.join(LCOH.path, "theoretical_loadshape_preserve.csv"))
#         self.load_8760 = np.array(dftheo["hload_dsg_lf"]) * 4868.746703
# =============================================================================
        
        self.peak_load = max(self.load_8760)
        self.fuelsavings = 0
        self.fuel_reduc = 0
        
        if self.measurements:
            self.fuel_reduc = pm.config["measurements"][str(pm.config["naics"])]["FUEL"]
            #self.fuelsavings = self.fuel_reduc * 8760 * 3600/self.hv_vals
            self.fuelsavings = 0
            self.load_8760 -= self.fuel_reduc
            
        self.load_avg = np.mean(self.load_8760)   

        self.state_name, self.state_abbr = pm.get_state_names(self.county)

        # convert $/mwh to $/kwh cap at facility max -> so no overprice
        self.pd_curve = pm.get_elec_curve(self.county, self.state_abbr)
        self.edrate = pm.get_demand_struc(self.county)

        self.fuel_price, self.fuel_year = gap.UpdateParams.get_fuel_price(
                                    self.state_abbr, self.fuel_type)

        self.fp_range = gap.UpdateParams.get_max_fp(self.state_abbr, self.fuel_type) - self.fuel_price
        

        self.fuel_esc = np.array([gap.UpdateParams.get_esc(self.state_abbr, self.fuel_type) / 100])
        
        self.landprice = pm.get_lp(self.county)
        self.landpricel = pm.get_lp(self.county, ag = True)
        self.lp_range = self.landprice - self.landpricel
        
        self.p_time = pm.config["ptime"]

        self.discount_rate = np.array([pm.config["discount"][self.tech_type]])
        
        self.OM_esc = pm.config["omesc"]
        
        self.elec_esc = pm.config["elecesc"]
        
    @abstractmethod
    def calculate_LCOH(self):

        print("Use attributes to calculate LCOH")

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
            self.p_time = np.array([self.p_time[0] for i in range(no_sims)])
            #self.p_time = np.random.randint(25,40,size = no_sims)
            self.discount_rate = np.random.uniform(0.05,0.15, size =no_sims)
            self.OM_esc = np.random.uniform(0,0.03,size = no_sims)
            self.fuel_esc = np.random.uniform(0,0.03,size = no_sims)
            self.rand_c = np.random.triangular(0.9,1,4,size = no_sims)
            self.rand_o = np.random.triangular(0.9,1,4,size = no_sims)
            self.rand_f = np.random.uniform(self.fuel_price, self.fuel_price + self.fp_range, size = no_sims)
            self.rand_l = np.random.uniform(self.landpricel, self.landprice, size = no_sims)
    
        if a_type.upper() == "TO":
            
            numvars = 8
            siml = int(no_sims/numvars)
            srange = pm.config["srange"]

            d_vals = [self.p_time[0], self.discount_rate[0], self.OM_esc[0], 
                      self.fuel_esc[0], 1, 1, self.fuel_price, self.landpricel]

            sim = [np.linspace(i*srange, i* (srange+1), num=siml) for i in d_vals]
            
# =============================================================================
#             #MC sim ranges below
#             sim =[np.linspace(25,40,num = siml), 
#                   np.linspace(0.05,0.15,num = siml),
#                   np.linspace(0,0.03,num = siml),
#                   np.linspace(0,0.03,num = siml),
#                   np.linspace(0.9,4,num = siml),
#                   np.linspace(0.9,4,num = siml),
#                   np.linspace(self.fuel_price,self.fuel_price + self.fp_range,num = siml),
#                   np.linspace(self.landpricel,self.landprice + self.lp_range, num = siml)
#                   ]
#             
# =============================================================================
            vals = [np.concatenate(([val]*i*siml, sim[i], [val]*(numvars-1-i)*siml), axis=0) for i, val in enumerate(d_vals)]
            
            self.p_time = [d_vals[0] for i in range(len(vals[0]))]
            #self.p_time = vals[0].astype(int)
            self.discount_rate = vals[1]
            self.OM_esc = vals[2]       
            self.fuel_esc = vals[3]
            self.rand_c = vals[4]
            self.rand_o = vals[5]   
            self.rand_f = vals[6]
            self.rand_l = vals[7]


class Greenfield(LCOH):

    def __init__(self, form):

        # Control variables
        LCOH.__init__(self, form)  
    
        # Model Import
        if (mp.deprc) and self.tech_type in ["FURNACE", "BOILER"]:
            self.mult = self.peak_load
            
        self.model = models.TechFactory.create_tech(
                     self.tech_type, self.county,
                     (self.mult, self.load_8760), 
                     (self.fuel_price,self.fuel_type)
                    )

        self.model.om()
        self.model.capital()

        def import_param():

            self.corp_tax = pm.get_corptax(self.state_name)
            
            self.subsidies = pm.get_subsidies(self.tech_type, self.county, self.state_abbr)
    
            self.depreciation = pm.get_dep_value
    
            def get_OM(t):
                
                """ Placeholders, OM fixed/var should be obtained from model.py"""
                # only need to initalize during init - first calculate_LCOH
                if not self.sim:
                
                    omp = 1
                    fmp = self.fuel_price
                    
                
                if self.sim:
                    
                    omp = self.rand_o
                    fmp = self.rand_f
                
                #
                self.fc = (self.model.fc - self.fuelsavings) * (fmp) * (1 + self.fuel_esc)**t
                
                def get_elec_cost():
                    #model.elec_gen refers to excess elec_gen beyond elec process heat consumption
                    diff = self.model.elec_gen - self.fe_load_8760
                    mit_cost = sum(self.model.elec_gen[diff <= 0] * self.pd_curve[0][diff <=0]) \
                                + sum(self.fe_load_8760[diff >0] *self.pd_curve[0][diff >0])
                    sell_cost = sum(diff[diff > 0] * self.pd_curve[1][diff > 0])
   
                    # hours in a month
                    length = np.array([31,28,31,30,31,30,31,31,30,31,30,31]) * 24
                    start = np.array([0,31,59,90,120,151,181,212,243,273,304,334])*24
                    # peak demand for each mont
                    peaks = []
                    peakind = []
                    
                    for month in range(len(length)):
                        ind = np.argmin(diff[start[month] : start[month] + length[month]])
                        peakind.append(ind)
                        peaks.append(self.model.elec_gen[ind])
                    
                    if type(self.edrate) == list:
                        peakrates = np.array([self.edrate[i] for i in peakind])

                        demand_cost = sum(np.array(peaks) * peakrates)
                    else:
                        demand_cost = sum(self.edrate*np.array(peaks))
 
                    return mit_cost + demand_cost
                
                # cost mitigated from selling electricity
                self.ec = -1 * get_elec_cost() *(1 + self.elec_esc)**t 
                
                if self.tech_type in ["PVEB", "PVRH"]:
                    # cost from purchasing electricity from grid
                    self.e_grid_costs = sum(self.model.load_remain/self.model.get_efficiency()*self.pd_curve[0]) *(1 + self.elec_esc)**t
                elif self.tech_type == "EBOILER":
                    #100% efficiency
                    self.e_grid_costs = sum(self.model.load_8760/self.model.get_efficiency()*self.pd_curve[0]) *(1 + self.elec_esc)**t
                else: 
                    self.e_grid_costs = 0
                    
                self.em_costs = self.model.em_costs
                
                if self.tech_type == "BOILER":
                    ompermitfees = sum(pm.config["permit"]["annual"][self.state_abbr])     
                    
                #ompermit fees are escalated using 2% - approx assumption of inflation since permit fees adjusted by CPI
                return np.array([(self.model.om_val) * omp * (1 + self.OM_esc) ** t + ompermitfees * (1.02)**t + 
                                 self.fc + self.ec + self.e_grid_costs + self.em_costs]).flatten()
                
            self.OM = get_OM
        
            def get_capital():
                
                try:
                    #add land price to capital cost (equipment) since land not part of cost
                    return [np.array([self.investment]), np.array([self.landpricel*self.model.landarea])]
                
                except AttributeError:
                    pass
              
                if not self.sim:
                    
                    cmp = 1
                    lmp = self.landpricel
                    
                if self.sim:
                    
                    cmp = self.rand_c
                    lmp = self.rand_l
                     
                    
                cap = np.array([self.model.cap_val]) * cmp
                # land prep costs : https://www.nrel.gov/docs/fy12osti/53347.pdf
                if self.tech_type not in ["BOILER", "EBOILER", "CHP", "FURNACE"]:
                    site_prep = 25000
                else:
                    site_prep = 0

                land = (lmp + site_prep) * np.array([self.model.landarea])
                
                if self.tech_type == "BOILER":
                    permitfees = sum(pm.config["permit"]["year0"][self.state_abbr])

                return [cap,land + permitfees]

            self.capital = get_capital
            
        import_param()


    def calculate_LCOH(self):

        """using general LCOH equation"""
        
        capital = self.capital()[0]
        land = self.capital()[1]

        subsidies = self.subsidies["p_cap"]* capital + self.model.sys_size * self.subsidies["size"]
        undiscounted = capital + land - subsidies
        total_d_cost = np.zeros(len(self.p_time))

        t_energy_yield = np.zeros(len(self.p_time))
        
        for ind, p_time in enumerate(self.p_time):
            for i in range(1, p_time+1):
                d_cost = (self.OM(i)[ind] *(1-self.corp_tax) - capital[ind] *
                self.depreciation(i, self.model.dep_year) * self.corp_tax) / \
                (1+self.discount_rate[ind]) ** i

                total_d_cost[ind] += d_cost
                
                energy_yield = sum(self.model.load_met) / (1 + self.discount_rate[ind]) ** i

                if self.tech_type in ["PVEB", "PVRH"]:
                    energy_yield = self.load_avg * 8760 / (1 + self.discount_rate[ind]) ** i

                t_energy_yield[ind] += energy_yield * (1-self.corp_tax)
                
            total_d_cost[ind] += self.model.decomm / (1+self.discount_rate[ind]) ** p_time
        # convert to cents USD/kwh  
        self.year0 = undiscounted
        return (undiscounted + total_d_cost)/t_energy_yield * 100

    def simulate(self, a_type, no_sims = 100):
        
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
                                                 "Land Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
            #initialize model capital and om attributes by running 1 calculate LCOH (default values)
            columns = list(mc_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.model.cap_val, self.model.om_val,
                         1, 1, self.fuel_price, self.landpricel, self.calculate_LCOH()]

            mc_results = mc_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)
            
            self.sim = True
            self.apply_dists("MC", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.model.cap_val, self.model.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f, 
                       self.landpricel + self.rand_l, self.calculate_LCOH()]
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
                                                 "Land Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
                    
            columns = list(to_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.model.cap_val, self.model.om_val,
                         1, 1, self.fuel_price, self.landpricel, self.calculate_LCOH()]
            
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)      
            
            self.sim = True                  
            self.apply_dists("TO", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.model.cap_val, self.model.om_val,
                       self.rand_c, self.rand_o, self.rand_f, 
                       self.rand_l, self.calculate_LCOH()]    
            #mc dist below
# =============================================================================
#             results = [self.p_time, self.discount_rate, self.OM_esc, 
#                        self.fuel_esc, self.model.cap_val, self.model.om_val,
#                        self.rand_c, self.rand_o, self.fuel_price + self.rand_f, 
#                        self.landpricel + self.rand_l, self.calculate_LCOH()]
# =============================================================================
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,results)})) 
            
            if not os.path.exists(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv")):
                to_results.to_csv(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv"))
            return to_results
        
class Replace(LCOH):

    def __init__(self, form):
        
        # assert tech type to be solar only
        LCOH.__init__(self, form) 
        
        # add assertion
        self.sf = pm.config["sf"]
        
        def get_target(mode):
            
            def get_sf(mult):
                ''' 
                returns difference between target sf and current sf
                '''
                if mult == 0:
                    return 0  - self.sf
                smodel = models.TechFactory.create_tech(
                         self.tech_type, self.county,
                         (mult, (1 - pm.config["td"]) * self.load_8760), 
                         (self.fuel_price,self.fuel_type)
                        )
                return sum(smodel.load_met)/sum(self.load_8760) - self.sf    
            
            def get_su(mult):
                smodel = models.TechFactory.create_tech(
                        self.tech_type, self.county,
                        (mult, (1 - pm.config["td"]) * self.load_8760), 
                        (self.fuel_price,self.fuel_type)
                        ) 
                return (smodel.su, sum(smodel.load_met)/sum(self.load_8760), mult)

            #   upper bound on bisection - MW of system
            upper = 5
            
            if mode == "default":
                return -1
            
            #code below- do while for target solar frac - sf will stop at target while su will use
            # sf setting as the minimum sf (lower bound) for root search
            mult = bisection(get_sf,0,upper,50)

            # get maximum possible solar fraction 
            while mult == None:
                self.sf -= 0.01
                mult = bisection(get_sf,0,upper,50)
                if self.sf <0:
                    print("Can't replace technology with any Solar Tech")
                    break            
           
            if mode == "sf":

                return mult
            
            if mode == "su":

                self.su_l, self.sf_l, self.mult_l = ([],[],[])
                
                # of data points for plotting - randomly threw meta data into here
                no = 50
                for i in np.linspace(mult,upper,no):
                    self.su_l.append(get_su(i)[0])
                    self.sf_l.append(get_su(i)[1])
                    self.mult_l.append(get_su(i)[2])
                su = np.array(self.su_l)
                return self.mult_l[np.argmax(su)]

        if self.mult >= 0:
            mult = self.mult
            self.smodel = models.TechFactory.create_tech(
                         self.tech_type, self.county,
                         (mult, (1 - pm.config["td"]) * self.load_8760), 
                         (self.fuel_price,self.fuel_type)
                        )
            if self.measurements:
                oldpeak = max(self.load_8760) + self.fuel_reduc
            else:
                oldpeak = max(self.load_8760)
            
            self.sf = sum(self.smodel.load_met)/sum(self.load_8760)

            self.dmodel = models.TechFactory.create_tech(
                         pm.config["comb"], self.county,
                         (oldpeak, self.smodel.load_remain), 
                         (self.fuel_price,self.fuel_type)
                        )
        else:
            #this mult value obtained through get_Target function has turndown ratio enforced already
            mult = get_target(pm.config["mode"])

            self.smodel = models.TechFactory.create_tech(
                         self.tech_type, self.county,
                         (mult, np.array(self.load_8760)), 
                         (self.fuel_price,self.fuel_type)
                        )
            if self.measurements:
                oldpeak = max(self.load_8760) + self.fuel_reduc
            else:
                oldpeak = max(self.load_8760)
            
            self.dmodel = models.TechFactory.create_tech(
                         pm.config["comb"], self.county,
                         (oldpeak, self.smodel.load_remain), 
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

    
            self.subsidies = np.array([pm.get_subsidies(pm.config["comb"], self.county, self.state_abbr), 
                                       pm.get_subsidies(self.tech_type, self.county, self.state_abbr)])

            self.corp_tax = pm.get_corptax(self.state_name)
    
            self.depreciation = pm.get_dep_value
    
            def get_OM(t):
                
                """ Placeholders, OM fixed/var should be obtained from model.py"""
                # only need to initalize during init - first calculate_LCOH
                if not self.sim:
                
                    omp = 1
                    fmp = self.fuel_price
                    
                
                if self.sim:
                    
                    omp = self.rand_o
                    fmp = self.rand_f
                
                #
                self.fc = (self.smodel.fc + self.dmodel.fc - self.fuelsavings) * (fmp) * (1 + self.fuel_esc)**t

                def get_elec_cost():
                    #refers to excess elec_gen
                    diff = self.smodel.elec_gen - self.fe_load_8760
                    mit_cost = sum(self.smodel.elec_gen[diff <= 0] * self.pd_curve[0][diff <=0]) \
                                + sum(self.fe_load_8760[diff >0] *self.pd_curve[0][diff >0])
                    sell_cost = sum(diff[diff > 0] * self.pd_curve[1][diff > 0])
                    
                    # hours in a month
                    length = np.array([31,28,31,30,31,30,31,31,30,31,30,31]) * 24
                    start = np.array([0,31,59,90,120,151,181,212,243,273,304,334])*24
                    
                    # peak demand for each mont
                    peaks = []
                    peakind = []
                    
                    for month in range(len(length)):
                        ind = np.argmin(diff[start[month] : start[month] + length[month]])
                        peakind.append(ind)
                        peaks.append(self.smodel.elec_gen[ind])

                    if type(self.edrate) == list:
                        peakrates = np.array([self.edrate[i] for i in peakind])
                        demand_cost = sum(np.array(peaks) * peakrates)
                    else:
                        demand_cost = sum(self.edrate*np.array(peaks))
      
                    return mit_cost + demand_cost   

                self.ec = -1 * get_elec_cost() *(1 + self.elec_esc)**t
    
                #  fuel price - multiply to convert the kW to total energy in a year (kW)
                #  divided by appropriate heating value 
                if self.measurements:
                    self.m_omcosts = \
                        [pm.config["measurements"][str(pm.config["naics"])][self.smodel.tech_type][1],
                         pm.config["measurements"][str(pm.config["naics"])][self.dmodel.tech_type][1]]
                    self.m_omcosts = [0,0]
                else:
                    self.m_omcosts = [0,0]

                if self.dmodel.tech_type == "BOILER":
                    ompermitfees = sum(pm.config["permit"]["annual"][self.state_abbr]) 

                return np.array([(self.smodel.om_val + self.dmodel.om_val + sum(self.m_omcosts)) * omp * \
                                 (1 + self.OM_esc) ** t + ompermitfees * (1.02)**t + self.fc + self.ec]).flatten()
                
            self.OM = get_OM

            def get_capital():
                
                try:
                    cap = np.array([0, self.investment]).reshape(-1,1)
                    land = np.array([0, self.landpricel*self.smodel.landarea]).reshape(-1,1)
                    return [cap,land]
                
                except AttributeError:
                    pass
              
                if not self.sim:
                    
                    cmp = np.array([1,1]).reshape(-1,1)
                    lmp = np.array([0,0]).reshape(-1,1)
                    
                if self.sim:
                    
                    cmp = np.array([np.ones(len(self.rand_c)), self.rand_c])
                    lmp = np.array([self.rand_l, self.rand_l])
                    
                site_prep = 25000    
                    
                cap = np.array([0, self.smodel.cap_val]).reshape(-1,1) 
                cap = np.multiply(cap, cmp)
                                    
                land = np.multiply(np.array([0, self.smodel.landarea]).reshape(-1,1), lmp + site_prep)
                return [cap,land]
                        

            self.capital = get_capital
            
        import_param()


    def calculate_LCOH(self):

        """using general LCOH equation"""
        
        #only count capital for solar technology 
        capital = self.capital()
        
        scapital = capital[0][1]
        dcapital = capital[0][0]
        sland = capital[1][1]
        dland = capital[1][0]

        subsidies = self.subsidies[1]["p_cap"]*scapital + self.smodel.sys_size * self.subsidies[1]["size"]
 
        undiscounted = scapital + dcapital + sland + dland - subsidies 

        total_d_cost = np.zeros(len(self.p_time))

        t_energy_yield = np.zeros(len(self.p_time))
        
        for ind, p_time in enumerate(self.p_time):
            for i in range(1, p_time+1):
                depreciation = np.array([self.depreciation(i, self.dmodel.dep_year), self.depreciation(i, self.smodel.dep_year)]).reshape(-1,1)
                d_cost = (self.OM(i)[ind] *(1+self.corp_tax) - np.sum(np.multiply(depreciation, capital[0]),0)[ind] * self.corp_tax) / \
                (1+self.discount_rate[ind]) ** i
                total_d_cost[ind] += d_cost

                energy_yield = self.load_avg * 8760 / (1 + self.discount_rate[ind]) ** i
                t_energy_yield[ind] += energy_yield

        # convert to cents USD/kwh  
        self.year0 = undiscounted
        return (undiscounted + total_d_cost)/t_energy_yield * 100

    def simulate(self, a_type, no_sims = 100):
        
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
                                                 "Land Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
            #initialize model capital and om attributes by running 1 calculate LCOH (default values)
            columns = list(mc_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.smodel.cap_val, self.smodel.om_val+self.dmodel.om_val,
                         1, 1, self.fuel_price, self.landpricel, self.calculate_LCOH()]
    

            mc_results = mc_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)
            
            self.sim = True
            self.apply_dists("MC", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.smodel.cap_val, self.dmodel.om_val + self.smodel.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f, 
                       self.landpricel + self.rand_l, self.calculate_LCOH()]
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
                                                 "Land Price",
                                                 "LCOH Value US c/kwh"
                                                 ])
                    
            columns = list(to_results.columns)
            structure = [self.p_time, self.discount_rate, self.OM_esc, 
                         self.fuel_esc, self.smodel.cap_val, self.smodel.om_val + self.dmodel.om_val,
                         1, 1, self.fuel_price, self.landpricel, self.calculate_LCOH()]
            
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,structure)}), ignore_index = True)      
            
            self.sim = True                  
            self.apply_dists("TO", no_sims)
            results = [self.p_time, self.discount_rate, self.OM_esc, 
                       self.fuel_esc, self.smodel.cap_val, self.smodel.om_val + self.dmodel.om_val,
                       self.rand_c, self.rand_o, self.fuel_price + self.rand_f,
                       self.landpricel + self.rand_l, self.calculate_LCOH()]
            to_results = to_results.append(pd.DataFrame({a:b for a,b in zip(columns,results)})) 
            
            if not os.path.exists(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv")):
                to_results.to_csv(os.path.join(LCOH.path, "tosim" + self.tech_type + ".csv"))
            return to_results
  
class LCOHFactory():
    @staticmethod
    def create_LCOH(form):
        try:
            if re.search("GREENFIELD", form[0].upper()):
                return Greenfield(form)
# =============================================================================
#             if re.search("EXTENSION",format):
#                 return Extension(format)
# =============================================================================
            if re.search("REPLACE", form[0].upper()):
                return Replace(form)
            raise AssertionError("No Such LCOH Equation")
        except AssertionError as e:
            print(e)
            
if __name__ == "__main__":
  
# =============================================================================
#     test = LCOHFactory().create_LCOH(("GREENFIELD", "PTCTES", 5, "39049"))
#     test.calculate_LCOH()
#     test.model.sys_size
#     test.model.om_val/test.model.sys_size
#     test.landpricel
# =============================================================================
    cost = []
    #''PVEB', "DSGLF", "PTC", "PTCTES","CHP", 
    for i in ['BOILER']:
        test1 = LCOHFactory().create_LCOH(('Greenfield', i, -1, '6037'))
        print(test1.calculate_LCOH())
# =============================================================================
#         for j in np.linspace(0.5,10.5,21):
#             test1 = LCOHFactory().create_LCOH(('REPLACE', i, j, '39049'))
#             cost.append(test1.calculate_LCOH())
#     cost
# =============================================================================
# =============================================================================
#     #script for meta data
#     for i in ["DSGLF", "PTCTES","PTC","PVEB"]:
#         test = LCOHFactory().create_LCOH(('REPLACE', i, False, '6059'))
#         test.calculate_LCOH()
#         df = pd.DataFrame()
#         df["mult"] = test.mult_l
#         df["sf"] = test.sf_l
#         df["su"] = test.su_l
#         lcoh = []
#         for j in test.mult_l:
#             obj = LCOHFactory().create_LCOH(('REPLACE', i, j, '6059'))
#             lcoh.append(obj.calculate_LCOH())
#         df["lcoh"] = lcoh
#         df.to_csv(i + "_lcohmeta.csv")
# =============================================================================

