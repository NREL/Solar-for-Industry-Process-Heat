# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 16:57:50 2020
Contains all the OM/capital cost as a function of temp bin, tech type, 
load bin - can just be a spreadsheet mapping stuff
@author: wxi

"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict, namedtuple, Counter
import math
import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from bisect import bisect
from emission_dicts import state_costs as sc


class Tech(metaclass=ABCMeta):
    path = "./calculation_data"
    cost_index = pd.read_csv(os.path.join(path, "cost_index_data.csv"), index_col = 0)

    @abstractmethod
    def __init__(self, county, hload, fuel, m_obj):

        self.fuel_price, self.fuel_type = fuel
        self.county = county
        self.mult = hload[0]
        self.peak_load = max(hload[1])
        self.load_8760 = hload[1]
        mp = m_obj
        self.h_vals = mp.hv_vals[self.fuel_type]
        self.state_name = mp.get_state_names(self.county)[0]
        self.br = mp.get_burden_rate(self.state_name)
        
    @abstractmethod
    def om(self):

        print("Returns the operating and maintenance costs (fixed+variable)")

    @abstractmethod
    def capital(self):

        print("Returns the capital cost.")
        
    @classmethod
    def index_mult(cls, index_name, year1, year2 = False):
        """ Deflates from price from year1 to year2 given an index_name"""
        try:
            if not year2:
                year2 = Tech.cost_index.loc[index_name,:].last_valid_index()
            return Tech.cost_index.loc[index_name, str(year2)] / Tech.cost_index.loc[index_name, str(year1)]
        
        except KeyError:
            
            lastyear = Tech.cost_index.loc[index_name,:].last_valid_index()
            firstyear= Tech.cost_index.loc[index_name,:].first_valid_index()
            if year1 < int(firstyear) or year2 > int(lastyear):
                    
                print("Please pick year between {} to {}".format(
                        Tech.cost_index.columns[0], Tech.cost_index.columns[-1]))
            else:
                    
                print("Not a valid cost index.")
    
    def solar_sizing(self, storeval = False):
        '''
        Sizes a solar technology for a given load based on sizing method. 
        Month is defined by 0 - 11 for Jan - Dec.
        
        '''
        filepath = self.mp.gen_dict[self.tech_type]
        month = self.mp.month
        
        df = pd.read_json(os.path.join(Tech.path, filepath))
        
        # Efficiencies used to translate input theoretical load to thermal energy required
        if self.tech_type in ["PTC", "PTCTES"]:
            eff = 0.98
        elif self.tech_type in ["PVRH"]:
            eff = 0.8 * 0.99 
        else: 
            eff = 1
            
        self.gen = np.array(df.loc[df["FIPS"] == int(self.county)]["gen"].values[0])
        monthlygen = np.array(df.loc[df["FIPS"] == int(self.county)]["monthlysums"].values[0])
        del df
        
        nohours = np.array([31,28,31,30,31,30,31,31,30,31,30,31]) * 24
        firsthour = np.array([0,31,59,90,120,151,181,212,243,273,304,334])*24
        
        if self.mult>=0:
            pass         
        elif self.mp.sizing == "techopp":
            peak_load = max(self.load_8760[firsthour[month] : firsthour[month] + nohours[month]])
            self.mult = peak_load * nohours[month]/(monthlygen[month] *eff)
        elif self.mp.sizing == "annual":
            demand = sum(self.load_8760)
            supply = sum(monthlygen) *eff
            self.mult = demand/supply
        elif self.mp.sizing == "peakload":
            peak_load = max(self.load_8760)            
            self.mult = peak_load / (1000 *eff)
        else:
            print("No Sizing Method Defined")

        self.gen = self.gen * self.mult
 
        # expect storeeval to be (storage size - kwh, fluid type)
        self.storeval = storeval
        
        # round trip efficiency dict. Electricity assume Li-ion
        rteff = {"elec": 0.86, "steam": 1, "hw" : 1}
        
        if storeval:
            #this variable represents flows
            self.store_8760 = []
            kw_diff = self.gen  - self.load_8760/eff
            # this variable represents the level of storage
            storage = 0
            storeff = rteff[self.fluid]
            for i in range(len(kw_diff)):
                val = kw_diff[i]
                if val > 0:
                    kw_diff[i] -= min(storeval-storage, val)
                    self.store_8760.append(min(storage+val, storeval))
                    storage = min(storage+val, storeval)
                    continue

                if (val < 0) & ((storage*storeff - abs(val)) >= 0):
                    storage += val/storeff
                    self.store_8760.append(val/storeff)
                    kw_diff[i] = 0
                    continue
                
                if (val < 0) & ((storage*storeff - abs(val)) < 0):
                    kw_diff[i] += storage*storeff
                    self.store_8760.append(-storage)
                    storage = 0
                    continue  
                if val == 0:
                    self.store_8760.append(val)
                    
            # new generation profile based on interaction with storage and load
            self.gen = self.load_8760/eff + kw_diff

        # excess electricity )
        self.elec_gen = (self.gen - np.array(self.load_8760)/eff).clip(min=0)
        
        # theoretical heating demand met
        self.load_met = np.array([min(i,j) for i,j in zip(self.load_8760, self.gen*eff)])
        
        # solar utilization
        if self.mult == 0:
            self.su = 0
        else:
            self.su = sum(self.load_met/eff)/sum(self.gen)

        # solar fraction
        self.sf = sum(self.load_met)/sum(self.load_8760)
        
        #check available county level land area
        self.avland = pd.read_csv("./calculation_data/county_rural_ten_percent_results_20200330.csv", usecols = [0,1])
        landareas = {"PVRH": 35208, "PVEB": 35208, "DSGLF": 3698, "SWH": 2024, "PTCTES": 16187 , "PTC": 8094}
        assert self.mult * landareas[self.tech_type]/10**6 <=  self.avland.loc[self.avland["County FIPS"] == int(self.county)]["County Area km2"].values[0], \
        "land area exceeded"
        
    def get_emission_cost(self, eff_map, target = False):
        '''
        target emissions is a function of peak load of boiler (size) will be 
        list of [NOX SOX PM] in ng/J
        compliance is on a 30 day rolling average 
        List element order is:
            0- all peak load nox
            1- <100 peak load so2
            2- 100-250 peak load so2
            3- >250 peak load so2
            4- <100 peak load PM
            5- 100-250 peak load PM
            6- >250 peak load PM
            
        Let assume pure coal is used for now 
        em targets are not boiler specific -> it's any combustion source
        Assume limits applied to boilers and CHP (doesn't apply to furnaces). 
        '''
        em_limits = [
                     {"COAL": 300, "PETRO": 129, "NG": 86 },
                     {"COAL": 87, "PETRO": 215, "NG": 10**6},
                     {"COAL": 87, "PETRO": 87, "NG": 10**6},
                     {"COAL": 65, "PETRO": 86, "NG": 86},
                     {"COAL": 22, "PETRO": 10**6, "NG": 10**6},
                     {"COAL": 22, "PETRO": 10**6, "NG": 10**6},
                     {"COAL": 22, "PETRO": 13, "NG": 13}
                    ]
        if target:
            em_target = target
        else:
            if self.peak_load * 0.00341 <100:
                #basically no limit
                nox_lim = 10**6
                sox_lim = em_limits[1][self.fuel_type]
                pm_lim = em_limits[4][self.fuel_type]
                
            elif 100 <= self.peak_load * 0.00341 <= 250:
                nox_lim = em_limits[0][self.fuel_type]
                sox_lim = em_limits[2][self.fuel_type]
                pm_lim = em_limits[5][self.fuel_type]
                
            else:
                nox_lim = em_limits[0][self.fuel_type]
                sox_lim = em_limits[3][self.fuel_type]
                pm_lim = em_limits[6][self.fuel_type]                

            em_target = [nox_lim, sox_lim, pm_lim]
            
        if self.tech_type == "BOILER":
            fuel_input_8760 = [a/b for a,b in zip(self.load_8760, eff_map)]
        if self.tech_type == "CHP":
            fuel_input = self.char["fuel_input"](self.peak_load)
            fuel_input_8760 = [fuel_input * eff for eff in eff_map]
        if self.tech_type == "FURNACE":
            # the emission factors are based on theoretical load
            fuel_input_8760 =  self.load_8760
        def get_emissions(fuel_input_8760, control = [False, False, False]):

            '''
            tech specific
            control = [NG, FUEL OIL, COAL]
            
            assigns annual emissions in tons to so2, nox, pm and an associated cost
            the cost will be based on title v permit costs + any mandatory reductions due to emission limits
            target emissions default to emission limits defined under NSPS
            Can also specify own emission standards if it is lower than NSPS
            
            no point analyzing on 30 day basis or yearly basis if using emission factors
            since em_factor(energy consumed) and em_limit (energy consumed) 
            '''
            # coal assume 0.7% weight https://www.engineeringtoolbox.com/classification-coal-d_164.html
            # assume residual oil: https://nefi.com/files/5515/2840/4209/Fuel_Sulfur_Content_Limitations_Fact_Sheet.pdf
            #^ can also extend to distillate vermont has similar guidelines
            t_energy = 3600 * sum(fuel_input_8760)  

            if self.tech_type in ("BOILER", "CHP"):
                
                sulfur_dict = {"COAL": 0.7, "PETRO": 0.3}

                if self.fuel_type == "NG":
                    # lb/(10^6 scf) to ton/kJ = 10^-6 * 0.0005 /1085
                    c_f = 10**(-6) * 0.0005/1085
                    
                    if self.peak_load * 0.00341 >= 100:
                        if control[0]:
                            nox_mult = c_f * 140
                    
                        else:
                            nox_mult = c_f * 190
                        
                    else:
                        if control[0]:
                            nox_mult = c_f * 50
                        else:
                            nox_mult = c_f * 100

                    try:
                        if self.chp_t == "gas":
                        # ppm to ton/kJ
                            nox_mult = 0.0182 * 10**(-6) * 0.000473909
                        else:
                            nox_mult = 0.0981 * 10**(-6) * 0.000473909

                    except AttributeError:
                        pass                
                            
                    sox_mult = 0.6 * c_f
                    pm_mult = 7.6 * c_f

                if self.fuel_type == "PETRO":
                    
                    c_f = 1/(153600*1000*1.055)*0.0005 # lb/(1000 gal) -> ton/kJ
                    
                    if self.peak_load * 0.00341 >= 100:
                        if control[0]:
                            nox_mult = c_f * 47
                        else:
                            nox_mult = c_f * 40

                    else:
                        nox_mult = t_energy * c_f * 55
                            
                    sox_mult = 157 * c_f * sulfur_dict[self.fuel_type]
                    pm_mult = (9.19 * sulfur_dict[self.fuel_type] +3.22) * c_f
                 
                if self.fuel_type == "COAL":
                    
                    c_f = 1/(20.739 * 1055055.85)*0.0005
                    
                    nox_mult  = 7.4 * c_f 
                    sox_mult = 38 * c_f * sulfur_dict[self.fuel_type]
                    pm_mult = 6 * c_f
                    
                    try:
                        if self.chp_t == "steam":
                        # ppm to ton/kJ
                            nox_mult = 0.192 * 10**(-6) * 0.000473909

                    except AttributeError:
                        pass   
            if self.tech_type == "FURNACE":
                # emission factors are based on fuel combustion but provided in /metric ton
                # of aluminum produced. Since input fuel_input_8760 is in theoretical we use theoretical
                #t_energy is in kJ -500 btu/lb ->1.163 *10**6 kJ/metric ton
                #ultimately nox_mult is in ton pollutant/kJ energy
                
                nox_mult = 1 / (1.163*10**6) * 0.63/1000
                sox_mult = 1 / (1.163*10**6) * 0.96/1000
                pm_mult = 1 / (1.163*10**6) * 0.27/1000

            # Assign emissions all in tons of emissions
            self.nox = nox_mult * t_energy
            self.nox_8760 = [nox_mult * fuel * 3600 for fuel in fuel_input_8760]
            self.sox = sox_mult * t_energy
            self.sox_8760 = [sox_mult * fuel * 3600 for fuel in fuel_input_8760]
            self.PM = pm_mult * t_energy
            self.PM_8760 = [pm_mult * fuel *3600 for fuel in fuel_input_8760]  
                
            self.em_t = self.nox + self.sox + self.PM

        get_emissions(fuel_input_8760)
        
        def em_reduc_costs(em_target, fuel_input_8760): 
            '''
            identify 30 day regions within 8760 hours where emissions
            do not comply and thus needs emission reduction costs  
            '''
            if self.tech_type == "FURNACE":
                return 0 
                
            def em_sum(val_8760):

                def m_avg(a, n=720):
                    ret = np.cumsum(a, dtype=float)
                    ret[n:] = ret[n:] - ret[:-n]
                    return ret[n - 1:]
                # in tons
                sum_8760 = m_avg(np.array(val_8760))

                return sum_8760
            
            size = len(em_sum(self.nox_8760))
            
            c_fac = 3600 *1000 * 1.10231 * 10 ** (-15)
            # target is in ng/J -> convert to ton by multiplying by energy use in 8760 by 30 day averages
            nox_diff = em_sum(self.nox_8760) - np.array([em_target[0] for i in range(size)]) * em_sum(fuel_input_8760) * c_fac
            sox_diff = em_sum(self.sox_8760) - np.array([em_target[1] for i in range(size)]) * em_sum(fuel_input_8760) * c_fac
            PM_diff = em_sum(self.PM_8760) - np.array([em_target[2] for i in range(size)]) * em_sum(fuel_input_8760) * c_fac
            
            nox_diff = max(nox_diff)
            sox_diff = max(sox_diff)
            PM_diff = max(PM_diff) 
            
            # NOX: https://www.epa.gov/sites/production/files/2015-11/documents/assessment_of_non-egu_nox_emission_controls_and_appendices_a_b.pdf
            # SOX: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.566.117&rep=rep1&type=pdf est eur -> usd
            # ESP: scrubber -> https://www3.epa.gov/ttn/naaqs/standards/pm/ria/riach-06.pdf
            
            em_reduc_cost = 0
            
            if nox_diff > 0:
                if self.tech_type == "BOILER":
                    em_reduc_cost += 2235 * nox_diff
                if self.tech_type == "CHP":
                    em_reduc_cost += 163 * nox_diff
            if sox_diff > 0:
                em_reduc_cost += 390 * sox_diff
            if PM_diff > 0:
                em_reduc_cost += 620 * PM_diff
            
            #print(em_reduc_cost)
            return em_reduc_cost
        
        reduc_cost = em_reduc_costs(em_target, fuel_input_8760)

        def get_reg_costs():
            '''
            not tech specific
            '''
            cost_dict = sc[self.state_name]

            try:
                #special usually means a complex cost system that I will capture in a lambda
                #any emission caps will be caught in the function
                return cost_dict["special"](self.em_t)
                
            except KeyError:
                
                pass
            
            if "cap" in cost_dict.keys():
                # variable cost cap
                cap = cost_dict["cap"]
            else:
                cap = 10**12
            
            if "spec_cap" in cost_dict.keys():
                
                spec_cap = cost_dict["spec_cap"]
                
                func = lambda x: min(spec_cap, x)
                
                em_t = sum(map(func, [self.nox, self.sox, self.PM]))
            else:
                em_t = self.em_t
            
            try:
                
                s_cost = cost_dict["stagger"]
                index = bisect(s_cost["lower"], em_t) - 1
                
                
                if ("fixed" and "variable")  in s_cost.keys():
                    
                    var_cost = s_cost["variable"][index] * em_t
                    fix_cost = s_cost["fixed"][index]
                    
                    if ("variable_cap" in s_cost.keys()):
                        if var_cost > s_cost["variable_cap"][index]:
                            var_cost = s_cost["variable_cap"][index]
                            
                    return min([var_cost,cap]) + fix_cost
                
                if "fixed" in s_cost.keys():
                    fix_cost = s_cost["fixed"][index]
                    var_cost = cost_dict["variable"][0] * em_t
            
                else: 
                    fix_cost = cost_dict["fixed"][0]
                    var_cost = s_cost["variable"][index] * em_t
                    
                    if ("variable_cap" in s_cost.keys()):
                        if var_cost > s_cost["variable_cap"][index]:
                            var_cost = s_cost["variable_cap"][index]                    
                
                return min([var_cost,cap]) + fix_cost
                    
            except KeyError:
            
                fix_cost = cost_dict["fixed"][0]
                var_cost = cost_dict["variable"][0] * em_t
            
                return min([var_cost,cap]) + fix_cost
            
        em_cost = get_reg_costs()

        return em_cost + reduc_cost
        

class DSGLF(Tech):

    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        self.fluid = "steam"
        self.tech_type = "DSGLF"
        
        self.solar_sizing()
        
        self.sys_size = 1200 * self.mult 
        
        self.fuel_type = False
        self.dep_year = 5
        self.elec_gen = np.array([0 for i in range(8760)])

    def om(self):

        year = 2010
        deflate_om = [Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Solar Field", year),
                      Tech.index_mult("Heat Exchangers and Tanks", year),
                      Tech.index_mult("Heat Exchangers and Tanks", year)]
        
        #perf_eng, plant equip ops, maint supervisor, TES maint supervisor, service cont-mirro wash, site maint, solar field maint, HTF maint, TES Maint
        om_breakdown = np.array([63000*(1+self.br), 40000*(1+self.br), 48000*(1+self.br), 48000*(1+self.br), 350000, 70485, 755274, 231642, 427410])
        cost_kw = om_breakdown/110000
        
        om = cost_kw * self.sys_size

        self.om_val = sum(a * b for a, b in zip(deflate_om, om)) * self.mult

        self.fc = 0
        self.decomm = 0
        self.em_costs = 0
        self.elec_gen = np.array([0 for i in range(8760)])
    
    def capital(self):
        """
        All costs from SAM - dsg lf direct steam commercial owner
        """
        year = 2018
        deflate_capital = [Tech.index_mult("Construction Labor", year), 
                           Tech.index_mult("Solar Field", year),
                           Tech.index_mult("Heat Exchangers and Tanks", year)]     
        
        # capital costs
        aperture = 3081.6
        siteimprov = aperture * 20
        solarfield = aperture * 150
        HTF = aperture * 35
        capex = [siteimprov,solarfield,HTF]
        
        self.landarea = 3698 * 0.000247105 * self.mult
        
        indirect = 0.185
        ctg = 0.1
        
        self.cap_val = (1+ctg + indirect) * sum(a * b for a, b in zip(deflate_capital, capex)) * self.mult * (1-self.mp.i_reduc)
                        
        
class PTC(Tech):

    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp) 
        
        self.tech_type = "PTC"
        self.solar_sizing()
        
        self.fuel_type = False
        self.dep_year = 5
        #this value is in kW
        self.sys_size = self.mult * 1500

    def om(self):

        year = 2010
        deflate_om = [Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Solar Field", year),
                      Tech.index_mult("Heat Exchangers and Tanks", year)]
        
        #perf_eng, plant equip ops, maint supervisor, service cont-mirro wash, site maint, solar field maint, HTF maint
        om_breakdown = np.array([63000*(1+self.br), 40000*(1+self.br), 48000*(1+self.br), 350000, 70485, 755274, 231642])
        cost_kw = om_breakdown/110000
        
        om = cost_kw * self.sys_size

        self.om_val = sum(a * b for a, b in zip(deflate_om, om)) * self.mult

        self.fc = 0
        self.decomm = 0
        self.em_costs = 0
        self.elec_gen = np.array([0 for i in range(8760)])
        
    def capital(self):

        year = 2018
        deflate_capital = [Tech.index_mult("Construction Labor", year), 
                           Tech.index_mult("Solar Field", year),
                           Tech.index_mult("Heat Exchangers and Tanks", year)]      
        
        aperture = 2624
        siteimprov = aperture * 25
        HTF = aperture*60
        solarfield = aperture * 150
        indirect = 0.185    
        
        self.landarea = 8094 * 0.000247105 *self.mult
        
        capex = [siteimprov,HTF,solarfield]

        ctg = 0.1

        self.cap_val = (1+ctg + indirect) * sum(a * b for a, b in zip(deflate_capital, capex)) * self.mult * (1-self.mp.i_reduc)
        

class PTCTES(Tech):
    
    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        self.tech_type = "PTCTES"        
        self.solar_sizing()             
        self.fuel_type = False
        self.dep_year = 5
        self.elec_gen = np.array([0 for i in range(8760)])
        #this value is in kW
        self.sys_size = self.mult * 2500     

    def om(self):
        '''
        All costs from SAM
        
        ''' 
        year = 2010
        deflate_om = [Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Engineering Supervision", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Construction Labor", year),
                      Tech.index_mult("Solar Field", year),
                      Tech.index_mult("Heat Exchangers and Tanks", year),
                      Tech.index_mult("Heat Exchangers and Tanks", year)]
        
        #perf_eng, plant equip ops, maint supervisor, TES maint supervisor, service cont-mirro wash, site maint, solar field maint, HTF maint, TES Maint
        om_breakdown = np.array([63000*(1+self.br), 40000*(1+self.br), 48000*(1+self.br), 48000*(1+self.br), 350000, 70485, 755274, 231642, 427410])
        cost_kw = om_breakdown/110000
        
        om = cost_kw * self.sys_size

        self.om_val = sum(a * b for a, b in zip(deflate_om, om)) * self.mult

        self.fc = 0
        self.decomm = 0
        self.em_costs = 0
        self.elec_gen = np.array([0 for i in range(8760)])
    
    def capital(self):
        """
        All costs from SAM
        """
        year = 2018
        deflate_capital = [Tech.index_mult("Construction Labor", year), 
                           Tech.index_mult("Solar Field", year),
                           Tech.index_mult("Heat Exchangers and Tanks", year),
                           Tech.index_mult("Heat Exchangers and Tanks", year)]      
        
        # capital costs -> 560$/kW
        aperture = 5248
        siteimprov = aperture * 25
        HTF = aperture*60
        solarfield = aperture * 150
        #62$/kwth
        storage = 62 * 6000
        indirect = 0.185
        self.landarea = 16187 * 0.000247105 * self.mult
        
        capex = [siteimprov, HTF, solarfield, storage]
        
        #contingency
        ctg = 0.1
        
        # capital value
        self.cap_val = (1+ctg + indirect) * sum(a * b for a, b in zip(deflate_capital, capex)) * self.mult * (1-self.mp.i_reduc)
        
    
class PVEB(Tech):
    """
       PV -> Electric Boiler AC
    """
    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        #assume 100% efficiency to thermal heat
        self.fluid = "elec"
        self.tech_type = "PVEB"
        self.solar_sizing()      
         
        self.fuel_type = False

        self.dep_year = 5
        #this value is in kWac
        self.sys_size = self.mult * 1000
        #for electric boiler sizing 
        self.design_load = max(self.load_met)

    def om(self):
        '''
        PV + EB costs
        PV: NREL ATB
        EB: times model
        storage electricity OM from NREL ATB
        
        '''
        
        year = 2018
        deflate_price = [Tech.index_mult("Engineering Supervision", year),
                         Tech.index_mult("Engineering Supervision", 2017),
                         Tech.index_mult("Engineering Supervision", year)] 
        
        # no variable OM from ATB/SAM
        om = self.sys_size * 1.2 * 16

        # https://www.epsalesinc.com/electric-boilers-vs-gas-boilers/ says no annual maint cost- > 0.01 as an estimate to cover maint+maintlabor
        om_eb = self.design_load * 61.02 * 0.01 #+ 62150 * 1.23 
        
        if self.storeval:
            om_batt = (self.storeval*335 + 4*292) * 0.025
        else: 
            om_batt=0
        # O&M, Fuel, Electricity Costs, Decomm Costs
        self.om_val = [a * b for a, b in zip(deflate_price, [om, om_eb, om_batt])]
        self.om_val = [self.om_val[0] + self.om_val[2], self.om_val[1]]
        
        self.fc = 0
        self.decomm = 0
        self.em_costs = 0
    
    def capital(self):
        """
        PV + EB costs
        PV: NREL ATB
        PV storage costs: NREL ATB
        """
        year = 2018
        deflate_capital = [Tech.index_mult("Solar Field", year), 
                           Tech.index_mult("Solar Field", year),
                           Tech.index_mult("Electrical equipment", year),
                           Tech.index_mult("Construction Labor", year),
                           Tech.index_mult("Engineering Supervision", year),
                           Tech.index_mult("CE INDEX", year),
                           Tech.index_mult("Boiler", 2017),
                           Tech.index_mult("BatteryStorage", year)]     
        # in order: Module, Inverter, balance of system, installation labor, permitting/environmental studies, overhead (get cost up to right number)
        cap_dict = {
                "utility50": (0.47,0.05,0.25,0.11,0.11,0.22),
                "utility10": (0.47,0.05,0.30,0.12,0.11,0.30), 
                "utility5": (0.47,0.05,0.30,0.12,0.11,0.30),
                "commercial": (0.32,0.07,0.25,0.12,0.11,0.65)
                }
        
        if self.sys_size > 50000:
            key = "utility50"
        elif self.sys_size >= 10000:
            key = "utility10"
        elif self.sys_size >= 5000:
            key = "utility5"
        else:
            key = "commercial"
            
        module, inverter, bos, instlab, permit, overhead = \
            (self.sys_size * 1000 * 1.2 * i * (1-self.mp.i_reduc) for i in cap_dict[key])
        
        capex_eb = self.design_load * 61.02 

        if self.storeval:
            capex_batt = (self.storeval*335 + 4*292)
        else:
            capex_batt = 0
        
        capex = [module, inverter, bos, instlab, permit, overhead, capex_eb, capex_batt]
        # contingency
        ctg = 0.2
        
        def la(hload):       
            """ 
            see the csv file with cleaver brooks/babcock boilers. Returns a value in quarter acre
            subtracts diff between normal boiler and eboiler size
            
            """
            hload = hload/293
            return ((1.68 *hload - 26.1) - (0.1176*hload + 1.1498)) / 4046.86 

        if self.mult == 0:
            self.landarea = 0
        else:
            self.landarea = 35208 * 0.000247105 * self.mult + la(self.peak_load) 

        # capital value
        self.cap_val = [a * b for a, b in zip(deflate_capital, capex)]
        self.cap_val = [sum(self.cap_val[0:6]) * (1+ctg), sum(self.cap_val[6:]) * (1+ctg)]
        
    def get_efficiency(self):
        return 1

    
class PVRH(Tech):

    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        #assume 100% efficiency to thermal heat
        self.fluid = "elec"
        self.tech_type = "PVRH" 
        self.solar_sizing() 
        self.fuel_type = False
        self.meltloss = 1
        #this value is in kWdc
        self.sys_size = self.mult * 1200  
        #this value for furnace - in tons/hr (Us tons)
        self.design_load = self.peak_load/293/ ((100 -self.meltloss)/100)
        self.dep_year = 5

    def om(self):
        
        '''
        PV + RH costs
        PV: NREL ATB
        RH: electrical cast melting in aluminum foundries
        
        '''
        
        year = 2018
        deflate_price = [Tech.index_mult("Engineering Supervision", year),
                         Tech.index_mult("Engineering Supervision", 2011),
                         Tech.index_mult("Aluminum", year)]


        # no variable OM from ATB/SAM
        om = self.sys_size * 16

        # rh om
        rh_maint = 32000/(1.05) * self.design_load
        meltcost = 1700 * sum(self.load_met)/293 * self.meltloss/(100-self.meltloss)
        #labor = 101434
        self.om_val = [a * b for a, b in zip(deflate_price, [om, rh_maint,meltcost])]
        #solar, conventional
        self.om_val = [self.om_val[0], sum(self.om_val[1:])]
        # O&M, Fuel, Electricity Costs, Decomm Costs        
        self.fc = 0
        self.decomm = 0
        self.em_costs = 0
    
    def capital(self):
        
        """
        PV + RH costs
        PV: NREL ATB
        """
        year = 2018
        deflate_capital = [Tech.index_mult("Solar Field", year), 
                           Tech.index_mult("Solar Field", year),
                           Tech.index_mult("Electrical equipment", year),
                           Tech.index_mult("Construction Labor", year),
                           Tech.index_mult("Engineering Supervision", year),
                           Tech.index_mult("CE INDEX", year),
                           Tech.index_mult("Furnace", 2011),
                           Tech.index_mult("BatteryStorage", year)]    
        
        # in order: Module, Inverter, balance of system, installation labor, permitting/environmental studies
  
        cap_dict = {
                "utility50": (0.47,0.05,0.25,0.11,0.11,0.22),
                "utility10": (0.47,0.05,0.30,0.12,0.11,0.30), 
                "utility5": (0.47,0.05,0.30,0.12,0.11,0.30),
                "commercial": (0.32,0.07,0.25,0.12,0.11,0.65)
                }
        
        if self.sys_size > 50000:
            key = "utility50"
        elif self.sys_size >= 10000:
            key = "utility10"
        elif self.sys_size >= 9500:
            key = "utility5"
        else:
            key = "commercial"
            
        module, inverter, bos, instlab, permit, overhead = \
            (self.sys_size * 1000 * i * (1-self.mp.i_reduc) for i in cap_dict[key])
            
        if self.storeval:
            capex_batt = (self.storeval*335 + 4*292)
        else:
            capex_batt = 0
            
        #add RH costs
        capex_rh = self.design_load * 553000/ 1.05
        
        capex = [module, inverter, bos, instlab, permit, overhead, capex_rh, capex_batt]       
        # contingency
        ctg = 0.2
        
        self.landarea = 42250 * 0.000247105 * self.mult
        
        # capital value
        self.cap_val =  [a * b for a, b in zip(deflate_capital, capex)]
        self.cap_val = [sum(self.cap_val[0:6])* (1+ctg), sum(self.cap_val[6:]) * (1+ctg)]
        
    def get_efficiency(self):
        return 0.8*(100-self.meltloss)/100   

class Boiler(Tech):
    
    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)

        # Imported values - 75% efficiency at lowest bound for sizing - margin of error for safety
        self.eff_dict = {"NG": [70,75], "COAL": [75,85], "PETRO": [72,80]}
            
        if self.mult > 0:
            self.peak_load = self.mult
            
        self.design_load = self.peak_load * 0.00341 / (self.eff_dict[self.fuel_type][1]/100) #specific to boilers
        self.dep_year = 15

        self.incrate = 0.1
        self.tech_type = "BOILER"
        self.sys_size = self.peak_load
        self.load_met = np.array(self.load_8760)
        self.capacity = sum(self.load_8760 > 0)/8760
        self.load_8760 = self.load_8760.clip(min = 0.25*self.peak_load)
        

        # Boiler Sizing Function
        def select_boilers():
            # extended coal packaged upper limit from 60 to 74 (not supposed to usually)
            # extended water tube packaged upper limit from 150 to 199
            heat_load = self.design_load
            
            boiler = namedtuple('Key', ['PorE', 'lower', 'upper', 'WorF'])

            coal_boilers = OrderedDict({
                            'A': boiler("packaged", 15, 74, "watertube"), 
                            'B': boiler("erected", 75, 199, "watertube"), 
                            'C': boiler("erected", 200, 700, "watertube")
                            })

            petro_ng_boilers = OrderedDict({
                                'A': boiler("packaged", 1, 4.99, "watertube"),
                                'B': boiler("packaged", 5, 29, "firetube"), 
                                'C': boiler("packaged", 30, 199, "watertube"), 
                                'D': boiler("erected", 200, 700, "watertube"),
                                })

            boiler_dict = {'COAL': coal_boilers, 'NG': petro_ng_boilers, 
                           'PETRO': petro_ng_boilers}

            def in_range(heat_load, lower, upper):
                for i in range(len(lower)):

                        if heat_load >= lower[i] and heat_load <= upper[i]:

                            return (i, heat_load)

                        elif heat_load <= lower[i]:

                            return (i, lower[i])
                        
                        else:
                            
                            continue

            lower = [l for a, l , u, b in boiler_dict[self.fuel_type].values()]

            upper = [u for a, l , u, b in boiler_dict[self.fuel_type].values()]

            boiler_list = []

            if heat_load > upper[-1]:

                while heat_load:

                        if heat_load - upper[-1] > 0:
                        
                                heat_load -= upper[-1]

                                boiler_list.append((-1,upper[-1]))
                        else:
                                boiler_list.append(in_range(heat_load, lower, upper))

                                heat_load = 0 
                                      
                return [list(Counter(boiler_list).keys()), list(Counter(boiler_list).values())]

            else:

                boiler_list.append(in_range(heat_load, lower, upper))   
  
                return [list(Counter(boiler_list).keys()), list(Counter(boiler_list).values())]
            
        self.boiler_list = select_boilers()
                
    def get_efficiency(self):

        """
        
            Efficiency defined by iea technology brief
            by fuel type (full load) - coal: 85% oil: 80% NG:75%
            quintic fit - since generalizability not necessary for 1 eff curve
            y = 11.534x5 - 34.632x4 + 39.736x3 - 21.955x2 + 6.2306x + 0.0967
        """

        
        def boiler_eff(pload):
            return -0.1574 * pload**2 + 0.2697 * pload + 0.7019
        
        if self.mp.boilereff:
            self.inceff = 1/(1- sum(self.mp.boilereffinc))
        else:
            self.inceff = 1
                    
        effmap = [min(boiler_eff(load/self.peak_load) * self.inceff,1)
                  for load in self.load_8760] 
        
        return effmap
    

    def om(self,capacity = 1, shifts = 1, ash = 6):

        """ 
        
            all the om equations do not include fuel or any sort of taxes
            labor cost cut by factor of 0.539 bc not designing new power plant - field-erected only
            - EPA cited
            - shifts = 1 person operates multiple boilers... assume just 1 large boiler
        """

        cap = capacity
        shifts = shifts
        ash = ash
        year = 1978
        
        # price deflation
        deflate_price = [
                         Tech.index_mult("Industrial Chemicals", year), 
                         Tech.index_mult("Engineering Supervision", year), 
                         Tech.index_mult("Engineering Supervision", 2018),
                         Tech.index_mult("Engineering Supervision", 2018),
                         Tech.index_mult("Engineering Supervision", 2018),
                         Tech.index_mult("Heat Exchangers and Tanks", year),
                         Tech.index_mult("Engineering Supervision", 2018),
                        ]
        
        def om(cap, shifts, ash, hload, count = 1):
            """Assume 5% from european report"""
            return 0.05*(25554*hload + 134363) * deflate_price[3] * count

        def om1(cap, shifts, hload, ash, HV = 20.739, count = 1): 

            u_c = cap / 0.60 * (hload / (0.00001105 * hload + 0.0003690)) * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3
            a_d = cap / 0.60 * 700 * hload * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3 * 0.38
            d_l = (shifts*2190)/8760 * (38020 * math.log(hload) + 28640) 
            s = (shifts*2190)/8760 * ((hload+5300000)/(99.29-hload)) 
            m = (shifts*2190)/8760 * ((hload+4955000)/(99.23-hload)) 
            r_p = (1.705 * 10 ** 8 *hload - 2.959 * 10 ** 8)**0.5 * (11800/(HV*500)) ** 1.0028 
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om2(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (29303 + 719.8 * hload)
            a_d = cap * 0.38 * (547320 + 66038 * math.log(ash/(HV*500))) * (hload/150) ** 0.9754
            d_l = (shifts*2190)/8760 * (202825 + 5.366*hload**2) * 0.539 
            s = (shifts*2190)/8760 * 136900 * 0.539 
            m = (shifts*2190)/8760 * (107003 + 1.873* hload**2) * 0.539 
            r_p = (50000 + 1000 * hload) * 0.539
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) * 0.539  

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om3(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (189430 + 1476.7 * hload)
            a_d = cap*(-641.08 + 70679828*ash/(HV*500))*(hload/200) ** 1.001 * 0.38
            d_l = (shifts*2190)/8760 * (244455 + 1157 * hload) * 0.539 
            s = (shifts*2190)/8760 * (243895 - 20636709/hload) * 0.539 
            m = (shifts*2190)/8760 *(-1162910 + 256604 * math.log(hload)) * 0.539 
            r_p = (180429 + 405.4 * hload) * 0.539
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) * 0.539  

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om4(cap, shifts, hload, ash, HV = 28.608, count = 1):
            shifts = self.capacity
            u_c = 771.36*hload + 2101.6 
            d_l = 262800 * shifts * 0 
            s = 0 
            m = 16000/105300 * 262800  * shifts
            r_p = 708.72 * hload + 4424.3 
            o = self.br *(d_l+m+s)  
            
            return sum(a * b for a, b in zip(deflate_price, [0, 0, 0, 0, m, r_p, o])) * count

        def om5(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap/0.55 * (202 * hload + 24262)
            d_l = (shifts*2190)/8760 * (hload**2/(0.0008135 * hload - 0.01585)) 
            s = (shifts*2190)/8760 * 68500 
            m = (shifts*2190)/8760 * (-1267000/hload + 77190) 
            r_p = 7185 * hload ** 0.4241
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        def om6(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (43671.7 + 479.6 * hload)
            d_l = (shifts*2190)/8760 * (173197 + 734 * hload) * 0.539 
            s = (shifts*2190)/8760 * (263250 - 30940000 / hload) * 0.539 
            m = (shifts*2190)/8760 * (32029 + 320.4 * hload) * 0.539
            r_p = (50000 + 250 * hload) * 0.539
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) * 0.539  

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        """ for now heating values done manually from Table_A5_...Coal in calculation_data"""
        
        # Assign om functions to appropriate location
        coal_om = OrderedDict({'A': om1, 'B': om2, 'C': om3})
        ng_om = OrderedDict({'A': om, 'B': om4, 'C': om5, 'D': om6})
        petro_om = OrderedDict({'A': om, 'B': om4, 'C': om5, 'D': om6})
        boiler_om = {'COAL': coal_om, 'NG': ng_om, 'PETRO': petro_om}
        
        # calculate om costs
        cost = 0
        for i in range(len(self.boiler_list[0])):
            cost += list(boiler_om[self.fuel_type].values())[self.boiler_list[0][i][0]](
            cap = cap, shifts = shifts / sum(self.boiler_list[1]) , ash = ash, hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])
       
        # all cost attributes
        if self.mp.boilereff:
            #cost data in 2019 dollars
            self.inceffom = sum(self.mp.boilereffom)
        else:
            self.inceffom = 0
            
        self.om_val = cost + self.inceffom
        self.elec_gen = np.array([0 for i in range(8760)])
        eff_map = self.get_efficiency()
        self.fc = (3600 * sum([a/b for a,b in zip(self.load_8760, eff_map)]) / self.h_vals)
        self.decomm = 0
        self.em_costs = self.get_emission_cost(eff_map)

    def capital(self):
        
        """ capital cost models do not include contingencies, land, working capital"""
        
        year = 1978

        deflate_capital = [Tech.index_mult("Heat Exchangers and Tanks", year), 
                           Tech.index_mult("Heat Exchangers and Tanks", year), 
                           Tech.index_mult("CE INDEX", year) ]
        
        def cap(hload, count = 1):
            """Osti model"""
        
            return (25554 * hload + 134363) * deflate_capital[0] * count
            
        def cap1(hload, HV = 28.608, count = 1):
            
            equip = 66392 * hload ** 0.622 * (11800/(HV*500)) + 2257 * hload ** 0.819
            inst = 53219 * hload ** 0.65 * (11800/(HV*500)) + 2882 * hload ** 0.796
            i_c = 40188 * hload ** 0.646 * (11800/(HV*500)) ** 0.926
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
            
        def cap2(hload, HV = 28.608, count = 1):
            
            equip = hload / (7.5963 * 10 ** -8 * hload + 4.7611 * 10 ** -5) * (HV*500/11800) ** -0.35
            inst = hload / (8.9174 * 10 ** -8 * hload + 5.5891 * 10 ** -5) * (HV*500/11800) ** -0.35
            i_c = hload / (1.2739 * 10 ** -7 * hload + 7.9845 * 10 ** -5) * (HV*500/11800) ** -0.35
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap3(hload, HV = 28.608, count = 1):
            
            equip = (4926066 - 0.00337 * (HV*500) **2) * (hload/200) ** 0.712
            inst = 1547622.7 + 6740.026 * hload - 0.0024133 * (HV*500) ** 2
            i_c = 1257434.72 + 6271.316 * hload - 0.00185721 * (HV*500) ** 2
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap4(hload, count = 1):

            # refer to boiler cost spreadsheet
            equip = 2540.8 * hload + 27867
            #everything excluding buildings
            inst = 2702.3 * hload + 28496
            i_c = 1677.8 * hload + 20036
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap5(hload, count = 1):
            
            equip = 14850 * hload ** 0.786
            inst = 54620 * hload ** 0.361
            i_c = 15952 * hload ** 0.618
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap6(hload, count = 1):
            
            equip = 1024258 + 8458 * hload
            inst = 579895 + 5636 * hload
            i_c = 515189 + 4524 * hload
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap7(hload, count = 1):
            
            equip = 17360 * hload ** 0.557
            inst = 4324 * hload + 56177
            i_c = 2317 * hload + 29749
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        
        def cap8(hload, count = 1):

            equip = 15920 * hload ** 0.775
            inst = 54833 * hload ** 0.364
            i_c = 16561 * hload ** 0.613
            
            return sum(a * b for a, b in zip(deflate_capital, [equip, inst, i_c])) * count
        

        """ for now heating values done manually from Table_A5_...Coal in calculation_data"""
        coal_cap = OrderedDict({'A': cap1, 'B': cap2, 'C': cap3})
        petro_cap = OrderedDict({'A': cap, 'B': cap4, 'C': cap5, 'D': cap6})
        ng_cap = OrderedDict({'A': cap, 'B': cap4, 'C': cap5, 'D': cap6})


        boiler_cap = {'COAL': coal_cap, 'NG': ng_cap, 'PETRO': petro_cap}
        
        cost_cap = 0
        for i in range(len(self.boiler_list[0])):
            cost_cap += list(boiler_cap[self.fuel_type].values())[self.boiler_list[0][i][0]] \
                        (hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])
       
        def la():

            def calc_area(hload, count = 1):
                """ see the csv file with cleaver brooks/babcock boilers. Returns a value in quarter acre"""
                return (1.68 *hload - 26.1) * count / 4046.86

            total_land_area = 0
            
            for i in range(len(self.boiler_list[0])):
                total_land_area += calc_area(hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])
            
            return total_land_area
        
        self.landarea = la()
        
        if self.mp.boilereff:
            #cost data in 2019 dollars
            self.inceffcap = sum(self.mp.boilereffcap)
        else:
            self.inceffcap = 0       
            
        # add 20% for contingencies as suggested by EPA report
        self.cap_val = cost_cap * 1.2 + self.inceffcap
        
        if self.mp.deprc:
            self.landarea = 0
            self.cap_val = 0

class Furnace(Tech):
    """
        Aluminum Melting Furnace Model
        #refer to model lit word doc for sources
        #for assume the load profile
    """
    
    def __init__(self, county, hload, fuel, m_obj):
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        if self.mp.furnaceeff:
            self.inceff = sum(self.mp.furnaceeffinc)
        else:
            self.inceff = 0

        # efficiency dictionary - function of melting furnace type - assume upper bound on reverb
        self.eff_dict = {"CRUCIBLE": 19, "REVERB": 35 * (1 + self.inceff) , "TOWER": 48}
        self.meltloss = {"CRUCIBLE": 3.5, "REVERB": 1.5 , "TOWER": 2}
        if self.mult > 0:
            self.peak_load = self.mult
        self.design_load = self.peak_load/293 / ((100 - self.meltloss[self.mp.furnace])/100)
        self.dep_year = 20
        self.incrate = 0.1
        self.tech_type = "FURNACE"
        self.sys_size = self.peak_load
        self.load_met = np.array(self.load_8760)
        self.load_8760 = self.load_8760.clip(min = 0.2*self.peak_load)
        self.plc = pd.read_csv('./calculation_data/furnacepload.csv')
     
    def get_efficiency(self):

        """
            Part load efficiency for aluminum furnace
        """

        def get_mult(pload):
            pos = np.searchsorted(np.array(self.plc["percent"]),pload)
            if pos == 0:
                return self.plc.loc[pos, "multiplier"]
            elif pos == 30:
                return 1
            else:
                uvalm = self.plc.loc[pos, "multiplier"]
                lvalm = self.plc.loc[pos-1, "multiplier"]
                uvalp = self.plc.loc[pos, "percent"]
                lvalp = self.plc.loc[pos-1, "percent"]
                slope = (uvalm-lvalm)/(uvalp-lvalp)

                return slope * (pload - lvalp) + lvalm
        #efficiency on 8760 basis 
        eff = self.eff_dict[self.mp.furnace] *  (100 -self.meltloss[self.mp.furnace])/100
        effmap = [min(eff / get_mult(load/self.peak_load *100) ,1) for load in self.load_8760]

        return effmap
    

    def om(self,capacity = 1, shifts = 1):

        """ 
            Aluminum Furnace OM Costs
            refer to cost spreadsheet
        """
        year = 2018
        #assume labor costs are not normalized to production rate - sum of averages instead
        # euro to usd in 2018 conversion : 1.18 usd to 1 euro 
        #normalized to standard (rated production rates) -direct labor not used
        labor = {"CRUCIBLE": 164950/self.design_load , "REVERB": 101434/self.design_load, "TOWER": 154178/self.design_load}
        maint_labor = {"CRUCIBLE": 4294, "REVERB": 4438, "TOWER": 5068}
        materials = {"CRUCIBLE": 30190, "REVERB": 3784, "TOWER": 6642}   
        
        #cost of lost aluminum based on theoretical since load in theoretical - 0.85/lb - assume no dross sale
        meltcost = 1700 * sum(self.load_8760)/293 * self.meltloss[self.mp.furnace] /(100 - self.meltloss[self.mp.furnace])/self.design_load

        # price deflation
        deflate_price = [
                         Tech.index_mult("Engineering Supervision", year),
                         Tech.index_mult("Furnace", year),
                         Tech.index_mult("Aluminum", year)
                        ]
        o_m = [maint_labor[self.mp.furnace], materials[self.mp.furnace], meltcost]

        self.om_val = sum(a * b for a, b in zip(deflate_price, o_m)) * self.design_load
        
        if self.mp.furnaceeff:
            self.inceffom = sum(self.mp.furnaceeffom)
        else:
            self.inceffom = 0
        
        self.om_val += self.inceffom
            
        self.elec_gen = np.array([0 for i in range(8760)])
        eff_map = self.get_efficiency()
        self.fc = (3600 * sum([a/b for a,b in zip(self.load_8760, eff_map)])/self.h_vals) # for holding  
        self.decomm = 0
        self.em_costs = self.get_emission_cost(eff_map)

    def capital(self):
        
        """ Aluminum Capital Costs"""
        
        year = 2018
        cap = {"CRUCIBLE": 147658 , "REVERB": 98607, "TOWER": 135659}
        deflate_capital = [Tech.index_mult("Furnace", year)]
        
       
        def la():
            '''
            get land area for the furnace in acres
            '''
            return 0
        self.landarea = la()
        
        if self.mp.furnaceeff:
            #cost data in 2019 dollars
            self.inceffcap = sum(self.mp.furnaceeffcap)
        else:
            self.inceffcap = 0   
            
        # add 20% for contingencies as suggested by EPA report
        self.cap_val = sum(a * b for a, b in zip(deflate_capital, [cap[self.mp.furnace]*self.design_load])) * 1.2 + self.inceffcap

        if self.mp.deprc:
            self.landarea = 0
            self.cap_val = 0      

class CHP(Tech):
    def __init__(self,county, hload, fuel, m_obj):
        """
        https://www.energy.gov/sites/prod/files/2016/04/f30/CHP%20Technical%20Potential%20Study%203-31-2016%20Final.pdf
        size to thermal load
        
        emissions - https://www.energy.gov/sites/prod/files/2016/09/f33/CHP-Steam%20Turbine.pdf
        https://www.energy.gov/sites/prod/files/2016/09/f33/CHP-Gas%20Turbine.pdf
        
        https://www.faberburner.com/resource-center/conversion-charts/emissions-conversions-calculator/
        emission conversions
        
        https://www.strata.org/pdf/2017/footprints-full.pdf : us power plant land use -> ng est for chp?
        """
        self.mp = m_obj
        Tech.__init__(self, county, hload, fuel, self.mp)
        
        # Not using sys 2 for energy model in excel file
        self.chp_t = self.mp.chp
        self.dep_year = 5
        self.tech_type = "CHP"
        # process csv file from data request. Use incidence rate as likelihood 
        self.incrate = 0.1
        self.load_met = np.array(self.load_8760)
        self.sys_size = self.peak_load
        def get_params(hload):
            # make linear extrapolation assumption
            # R^2 - i_om = 0.782
            if self.chp_t == "gas":
                self.useful_t = np.array((19.6, 36.3, 52.2, 77.4, 133.8))*293
                self.char = {"nom_ep(kW)": np.array((3515, 7965, 11350, 21745, 43067)),
                             "net_ep": np.array((3304, 7487.1, 10669, 20440.3, 40482.98)),
                             "fuel_input": np.array((47.5, 87.6, 130, 210.8, 389)) * 293,
                             "p_to_h": np.array((0.58, 0.7, 0.7, 0.9, 1.03)),
                             "e_eff(%)": np.array((23.7, 29.2, 28, 33.1, 35.5)),
                             "t_eff": np.array((0.411, 0.427, 0.414, 0.367, 0.344)),
                             "o_eff": np.array((0.648, 0.706, 0.682, 0.698, 0.699)),
                             "i_equip": np.array((6528100, 8996800, 12214900, 19397900, 35134910)),
                             "i_install": np.array((2204000, 2931400, 3913700, 6002200, 10248400)),
                             "i_other": np.array((2107700, 2707400, 3535600, 4264200, 10123300)),
                             "i_decomm": np.array((1632025,2249200,3053725,4849475,8783728)),
                             "i_om": np.array((0.013,0.012,0.012,0.009,0.009)) #$/kWh
                            }

            if self.chp_t == "steam":
                self.useful_t = np.array((20, 155.5, 506.8))*293
                self.char = {"net_ep": np.array((500, 3000, 15000)),
                             "fuel_input": np.array((27.2, 208, 700.1))*293,
                             "steam_flow(lb/hr)": np.array((20050, 152600, 494464)),
                             "p_to_h": np.array((0.086, 0.066, 0.101)),
                             "useful_elec(MMBTU/hr)": np.array((1.72, 10.263, 51.1868)) * 293,
                             "t_eff%": np.array((73.3, 74.8, 72.4)),
                             "o_eff": np.array((79.6, 79.7, 79.7)),
                             "i_equip": np.array((334000, 1203000, 5880000)),
                             "i_install": np.array((568000, 2046000, 9990000)),
                             "i_other": np.array((0,0,0)),
                             "i_decomm": np.array((83500,300750,1470000)),
                             "i_om": np.array((0.01, 0.009, 0.006)) #$/kWh
                            } 

            model = LinearRegression(fit_intercept = False)   
            # update each equation with an interpolation          
            for i in self.char.keys():
                if i == "i_om":
                    model = LinearRegression(fit_intercept = True)
                temp_model = model.fit(self.useful_t.flatten().reshape(-1,1), self.char[i].flatten().reshape(-1,1))
                coeff = float(temp_model.coef_)
                intercept = float(temp_model.intercept_)
                self.char[i] = lambda x, coeff = coeff, intercept = intercept: coeff * x  + intercept  
                if i == "i_om":
                    model = LinearRegression(fit_intercept = False)

        get_params(self.peak_load)
        
    def om(self):
        #define fuel eff as a func of load_replaced
        year = 2017
        load_replaced = [(self.peak_load - i)/self.peak_load for i in self.load_8760]

        eff_map = list(map(self.get_efficiency, load_replaced))
        
        # removing count for now -this unit is in kwh so cost needs to be per kwh
        self.elec_gen = self.avail_fac * np.array([self.char["net_ep"](self.peak_load) * i/self.peak_load for i in self.load_8760])
        # since p2h ratio same, just multiply by useful power initially
        net_ep = sum(self.elec_gen)
        self.om_val = self.char["i_om"](self.peak_load) * net_ep * Tech.index_mult("Engineering Supervision", year)
        
        self.decomm = self.char["i_decomm"](self.peak_load) * Tech.index_mult("CHP", year)
        
        fuel_input = self.char["fuel_input"](self.peak_load)
        self.fc = self.avail_fac * (3600 * sum([fuel_input * eff for eff in eff_map]) / self.h_vals)


        self.em_costs = self.get_emission_cost(eff_map)
        
    def capital(self):
        year = 2017
        deflate_capital = [Tech.index_mult("CHP", year), Tech.index_mult("CHP", year), Tech.index_mult("CE INDEX", year)]
        ctg = 0.2 #contingency
        i_equip = self.char["i_equip"](self.peak_load)
        i_install = self.char["i_install"](self.peak_load)
        i_other = self.char["i_other"](self.peak_load)
        
        #0.343 acres per MW
        self.landarea = 0.343 * self.char["net_ep"](self.peak_load)/1000
        
        self.cap_val = (1+ctg) * sum(a*b for a,b in zip(deflate_capital, [i_equip + i_install + i_other]))
        
    def get_efficiency(self, load_replaced):
        
        if self.chp_t == "gas":
            """provides partial load of CHP for a given system and % load replaced (p_load) """
            index = bisect(self.useful_t, self.peak_load)
            if not index:
                sys = 0
            elif index == len(self.useful_t):
                sys = len(self.useful_t) - 1
            elif abs(self.peak_load - self.useful_t[index-1]) >= abs(self.useful_t[index] - self.peak_load):
                sys = index
            else:
                sys = index-1

            interp_results = {0 : (0.3459, 0.0792), 1: (0.3531, 0.0808), 2: (0.3386, 0.0775), 3: (0.3114, 0.0713), 4: (0.2918, 0.0668)}
            c = -1 * self.peak_load * (1-load_replaced)/self.char["fuel_input"](self.peak_load)
            a = interp_results[sys][0]
            b = interp_results[sys][1]
            return min((-b + math.sqrt(b**2 - 4*a*c))/(2*a),1)

        if self.chp_t == "steam":

            if load_replaced > 0.6:
                load_replaced = 0.6
            index = bisect(self.useful_t, self.peak_load)
            if not index:
                sys = 0
            elif index == len(self.useful_t):
                sys = len(self.useful_t) - 1
            elif abs(self.peak_load - self.useful_t[index-1]) >= abs(self.useful_t[index] - self.peak_load):
                sys = index
            else:
                sys = index-1

            interp_results = {0: (-0.662, 1.2608, 0.1309), 1: (-0.6755, 1.2866, 0.1336), 2: (-0.6539, 1.2453, 0.1293)}
            a = interp_results[sys][0]
            b = interp_results[sys][1]
            c = interp_results[sys][2]
            d = -1 * self.peak_load * (1-load_replaced) / self.char["fuel_input"](self.peak_load)
            eq = [a,b,c,d] 
            roots = np.roots(eq)
            mask = (roots <= 1) * (roots >= 0)
            
            return min(roots[mask][0],1)

class TechFactory():
    @staticmethod
    def create_tech(form,county,hload,fuel, m_obj):
        try:
            if form.upper() == "BOILER":
                return Boiler(county,hload,fuel, m_obj)
            if form.upper() == "FURNACE":
                return Furnace(county,hload,fuel, m_obj)
            if form.upper() == "CHP":
                return CHP(county,hload,fuel, m_obj)
            if form.upper() == "PTC":
                return PTC(county,hload,fuel, m_obj)
            if form.upper() == "PTCTES":
                return PTCTES(county,hload,fuel, m_obj)
            if form.upper() == "DSGLF":
                return DSGLF(county,hload,fuel, m_obj)
            if form.upper() == "PVEB":
                return PVEB(county,hload,fuel, m_obj)
            if form.upper() == "PVRH":
                return PVRH(county,hload,fuel, m_obj)
            raise AssertionError("No Such Technology")
        except AssertionError as e: 
            print(e)
