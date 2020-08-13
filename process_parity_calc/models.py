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
    land_price = pd.read_csv(os.path.join(path, "landprices.csv"), index_col = 0)
    fips_data = pd.read_csv(os.path.join(path, "US_FIPS_Codes.csv"),
                            usecols=['State', 'COUNTY_FIPS', 'Abbrev']) 

    @abstractmethod
    def __init__(self, county, hload, fuel):
        self.fuel_price, self.fuel_type = fuel
        self.county = county
        self.peak_load = hload[0]
        self.load_8760 = hload[1]
        self.hv_vals = {"PETRO" : 4.641, "NG" : 1039, "COAL" : 20.739}
        # convert heating_values to the appropriate volume/mass basis and energy basis (kW)
        self.hv_vals["PETRO"] = 4.641 * 1055055.85 / 42 # / to gallon, * to kJ
        self.hv_vals["NG"] = 1039 * 1.05506 / 0.001 # / to thousand cuf * to kJ
        self.hv_vals["COAL"] = 20.739 * 1055055.85 # already in short ton, * to kJ
        # Land Price
        self.valperacre = Tech.land_price.loc[Tech.land_price["County"] == int(self.county), "Price (1/4 acre)"].max()
        self.state_name = Tech.fips_data.loc[Tech.fips_data['COUNTY_FIPS'] == self.county,
                                        'State'].values[0].strip()
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
    
    def solar_sizing(self, filepath, month, storeval = False):
        '''
        peak load will be defined by month for the solar technologies
        
        filepath to json object
        
        month on 0 - 11 for jan - dec
        
        '''
        df = pd.read_json(os.path.join(Tech.path, filepath))
        self.gen = df.loc[df["FIPS"] == self.county]["gen"][0]
        monthlysums = df.loc[df["FIPS"] == self.county]["monthlysums"][0]
        del df
         

        length = np.array([31,28,31,30,31,30,31,31,30,31,30,31]) * 24
        start = np.array([0,31,59,90,120,151,181,212,243,273,304,334])*24
            
        peak_load = max(self.load_8760[start[month] : start[month] + length[month]])
        
        self.mult = np.ceil(peak_load * length[month]/monthlysums[month])
        
        self.gen = self.gen * self.mult
        
        self.storeval = storeval
        
        # steam accumulator in ps10 can capture 50% of load for 50 min (20 Mwh system) for 11 Mw tower
        # assume efficiency is 25%, apply efficiency on removal
        if storeval:
           
            self.store_8760 = []
            kw_diff = self.gen - self.load_8760
            storage = 0
            eff = 0.25
            for i in range(len(kw_diff)):
                val = kw_diff[i]
                if val > 0:
                    kw_diff[i] -= min(storeval-storage, val)
                    self.store_8760.append(min(storage+val, storeval))
                    storage = min(storage+val, storeval)
                    continue

                if (val < 0) & ((storage*eff - abs(val)) >= 0):
                    storage += val/eff
                    self.store_8760.append(val/eff)
                    kw_diff[i] = 0
                    continue
                
                if (val < 0) & ((storage*eff - abs(val)) < 0):
                    kw_diff[i] += storage*eff
                    self.store_8760.append(-storage)
                    storage = 0
                    continue  
                if val == 0:
                    self.store_8760.append(val)
                    
        self.gen = self.load_8760 + kw_diff
        
        self.load_remain = (np.array(self.load_8760) - self.gen).clip(min=0)
        
    def gf_size(self):
        
        # redo the previous sizing on the generation
        self.gen = self.gen/self.mult

        ratio = np.array(self.load_8760[self.gen>0]) / self.gen[self.gen>0] 
        storeval = np.linspace(1,20,20)
        mult = np.array(range(1,np.ceil(max(ratio)) + 1))
        
        X,Y = np.meshgrid(storeval, mult)
        
        
        @np.vectorize
        def get_load_rp(mult, storeval, done = False):
            store_8760 = []
            kw_diff = self.gen *mult - self.load_8760
            storage = 0
            eff = 0.25
            for i in range(len(kw_diff)):
                val = kw_diff[i]
                if val > 0:
                    kw_diff[i] -= min(storeval-storage, val)
                    storage = min(storage+val, storeval)
                    store_8760.append(min(storage+val, storeval))
                    continue

                if (val < 0) & ((storage*eff - abs(val)) >= 0):
                    storage += val/eff
                    kw_diff[i] = 0
                    store_8760.append(val/eff)
                    continue
                
                if (val < 0) & ((storage*eff - abs(val)) < 0):
                    kw_diff[i] += storage*eff
                    storage = 0
                    store_8760.append(-storage)
                    continue  
                if val == 0:
                    store_8760.append(val)
                    continue
                
            if done:
                return (kw_diff + self.load_8760, store_8760)
            
            return (sum(self.load_8760) - sum(kw_diff[kw_diff<0])) /(sum(self.load_8760))
        
        # get max Z and the  X,Y corresponding to it
        Z = get_load_rp(X,Y)
        
        
        if np.max(Z) <= 0.9:
            print("Greenfield Investment is not Suggested")

        # check for local maximum
        pairs = np.argwhere(Z>0.9)
        self.storeval = storeval[pairs[0][0]]
        self.mult = mult[pairs[0][1]]

        self.gen , self.store_8760 = get_load_rp(self.storeval, self.mult, done = True)
        
        self.load_remain = (self.load_8760 - self.gen).clip(min = 0)

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
        
# =============================================================================
#         [(0,9999999, "COAL", "PM", 22, "PURE"), 
#                      (0,9999999, "COAL", "PM", 43, "MIXED"), 
#                      (0,99, "COAL", "PM", 43, "LOW"), 
#                      (100,9999999, "COAL", "PM", 86, "LOW"),
#                      (0,9999999, "WOOD", "PM", 43), 
#                      (251,9999999, "NG", "PM", 13),
#                      (251,9999999, "OIL", "PM", 13)]  
# =============================================================================
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

        def get_emissions(fuel_input_8760, control = [False, False, False]):

            '''
            CHECK VALIDITY OF NUMBERS -> IF NOT WE HAVE TO DO FACILITY MAPPING

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
            if self.tech_type in ("BOILER", "CHP"):
                
                sulfur_dict = {"COAL": 0.7, "PETRO": 0.3}
                t_energy = 3600 * sum(fuel_input_8760)

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
                # Assign emissions
                self.nox = nox_mult * t_energy
                self.nox_8760 = [nox_mult * fuel * 3600 for fuel in fuel_input_8760]
                self.sox = sox_mult * t_energy
                self.sox_8760 = [sox_mult * fuel * 3600 for fuel in fuel_input_8760]
                self.PM = pm_mult * t_energy
                self.PM_8760 = [pm_mult * fuel *3600 for fuel in fuel_input_8760]  
            
            # check if its solar
            if self.tech_type == "PVHP":
                self.nox = 0
                self.sox = 0
                self.PM = 0
                
            self.em_t = self.nox + self.sox + self.PM

        get_emissions(fuel_input_8760)
        
        def em_reduc_costs(em_target, fuel_input_8760): 
            '''
            identify 30 day regions within 8760 hours where emissions
            do not comply and thus needs emission reduction costs  
            '''
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
            
            #print(nox_diff, sox_diff, PM_diff)
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
        
        # check if solar
        if self.tech_type == "PVHP":
            em_cost = 0
            reduc_cost = 0

        return em_cost + reduc_cost
        
class PVHP(Tech):
    """
       Steven's PVHP model
    """
    def __init__(self, county, hload, fuel):
        
        Tech.__init__(self, county, hload, fuel)        
        # Simulation File and Cost Index File
        self.simr = pd.read_csv(os.path.join(Tech.path,"pvhp_sim.csv"))
        self.fuel_type = False
        self.tech_type = "PVHP"
        # Sizing Function
        def select_PVHP():
            
            self.sfid = self.simr["Solar Fraction"].idxmax()
            self.sflcoh = self.simr.loc[self.sfid, "LCOH ($/kWh)"]
            
            self.lcohid = self.simr["LCOH ($/kWh)"].idxmin()
            self.lcoh = self.simr["LCOH ($/kWh)"].min()
            
            #use lowest lcoh by default

            self.sfcount = math.ceil(self.peak_load / self.simr.loc[self.sfid, "PVHP Design Output (kWth)"])
            self.lcohcount = math.ceil(self.peak_load / self.simr.loc[self.lcohid, "PVHP Design Output (kWth)"])
            
            self.dep_year = 5
            
        select_PVHP()
        
        # process csv file from data request. Use incidence rate as likelihood 
        self.incrate = 0.1

    def om(self):
        '''
        source: https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2016/160649r.pdf
        26$/kwp-yr for O&M
        '''
        
        year = 2015
        deflate_price = [Tech.index_mult("Equipment", year)] 
        
        # Direct labor costs
        #o_m = 0.6/1.6 * self.simr.loc[self.lcohid, "CapEx ($)"]
        o_m = self.simr.loc[self.sfid, " PVkW Installed (kWel)"] * 26
        # O&M, Fuel, Electricity Costs, Decomm Costs
        self.om_val = sum(a * b for a, b in zip(deflate_price, [o_m])) * self.sfcount
        self.fc = 0
        self.elec_gen = 0
        self.decomm = 0
        self.em_costs = 0
    
    def capital(self):
        
        year = 2018
        deflate_capital = [Tech.index_mult("Equipment", year)]     
        
        # capital costs
        equip = self.simr.loc[self.sfid, "CapEx ($)"] 
        
        # contingency
        ctg = 0.2
        
        # capital value
        self.cap_val = (1+ctg) * sum(a * b for a, b in zip(deflate_capital, [equip])) * self.sfcount
        
    def get_efficiency(self):

        """
        PVHP thermal efficiency 
        
        """
        # returns 0 because assume that Solar doesn't use fuel
        
        return 1

class DSGLF(Tech):
    """
       direct steam generation - linear fresnel PTC
       get generation and sizing using parts of the rev processing file and the 8760 input demand file
       get storage cost for this particular system
    """    
    def __init__(self, county, hload, fuel):
        
        Tech.__init__(self, county, hload, fuel)     
        self.solar_sizing("dsg_lf_gen.json",0)
        
        
class PTC(Tech):
    """
       Parabolic Trough Collector - with or without TES 6hs
    """  
    def __init__(self, county, hload, fuel):
        
        Tech.__init__(self, county, hload, fuel)     
        self.solar_sizing("ptc_notes_gen.json",0)
        
class PTCTES(Tech):
    
    """
       Parabolic Trough Collector - with or without TES 6hs
    """ 
    def __init__(self, county, hload, fuel):
        
        Tech.__init__(self, county, hload, fuel)   
        self.solar_sizing("ptc_tes_gen.json",0)             
        
# =============================================================================
# class SWH(Tech):
#     """
#        solar water heater
#     """ 
#     def __init__(self, county, hload, fuel):
#         
#         Tech.__init__(self, county, hload, fuel)     
# 
# class PVEB(Tech):
#     """
#        PV -> Electric Boiler
#     """
#     def __init__(self, county, hload, fuel):
#         
#         Tech.__init__(self, county, hload, fuel) 
# class PVRH(Tech):
#     """
#        PV -> resistance heating
#    """  
#     def __init__(self, county, hload, fuel):
#         
#         Tech.__init__(self, county, hload, fuel)      
# class PVWHR(Tech):
#     """
#         PV -> waste heat recovery
#     """
#     def __init__(self, county, hload, fuel):
#         
#         Tech.__init__(self, county, hload, fuel) 
# 
# =============================================================================

class Boiler(Tech):
    """
        Using the EPA report from 1978 to populate this boiler model. The EPA
        report has various cost curves for different fuel types/loads. 
        - design heat input rate not the actual output rate- efficiency matters

        https://www1.eere.energy.gov/femp/pdfs/OM_9.pdf

        boiler best practice is to operate near full load - assume that controls
        are in place for modular boiler systems to manage varying load
        - ie blanket efficiency near peak
        
        Assumption - use 1 boiler unless forced to use multiple. In the future
        for specific deep dive case studies investigate multiple packaged vs
        1 field erected or multiple smaller vs 1 large
        
        https://www.epa.gov/sites/production/files/2015-12/documents/iciboilers.pdf
        ^pulverized>stoker in terms of efficiency
        
        under 15 M BTU or 5 M BTU/hr 
        the numbers appear accurate for purchased equipment cost- 
        
        https://iea-etsap.org/docs/TIMES_Dispatching_Documentation.pdf - multiple unit
        
        <5 MMBTU - installed cost Table page 61
        https://www.osti.gov/servlets/purl/797810
        
        Default boiler efficiency -> new boilers no improvements 
        
        https://www.iea-etsap.org/E-TechDS/PDF/I01-ind_boilers-GS-AD-gct.pdf -
        economizer improves by 5% for 2.3 million (650 MMBtu/hr) -> 30 MMBtu/hr only
        
        https://www.epa.gov/sites/production/files/2015-12/documents/iciboilers.pdf
        
        ESTCP -> cost performance report
        
        APH + economizer -> aph not used due to NOX increase
    """
    
    def __init__(self,county, hload,fuel):
        
        Tech.__init__(self, county, hload, fuel)
        
        """ Temp in Kelvins, heat load in kW """
        self.economizer = False
        
        # Imported values
        self.eff_dict = {"NG": [70,75], "COAL": [75,85], "PETRO": [72,80]}
        
        if self.economizer:
            self.eff_dict = {"NG": [75,80], "COAL": [80,90], "PETRO": [77,85]}
            
        # Format String processing
        self.design_load = self.peak_load * 0.00341 / (self.eff_dict[self.fuel_type][1]/100) #specific to boilers
        self.dep_year = 15
        
        # process csv file from data request. Use incidence rate as likelihood 
        self.incrate = 0.1
        
        self.tech_type = "BOILER"

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
            
        self.incrate = 0.1      
    def get_efficiency(self):

        """
        
            Efficiency defined by iea technology brief
            by fuel type (full load) - coal: 85% oil: 80% NG:75%
        
        """
        
        # Linear interpolation between min and maxeff
        maxeff = self.eff_dict[self.fuel_type][1]
        mineff = self.eff_dict[self.fuel_type][0]
        effmap = [(mineff + (maxeff-mineff) * load/self.peak_load) / 100 for load in self.load_8760] 
        
        return effmap
    

    def om(self,capacity = 1, shifts = 0.75, ash = 6):

        """ 
        
            all the om equations do not include fuel or any sort of taxes
            labor cost cut by factor of 0.539 bc not designing new power plant - field-erected only
            - EPA cited
            - use only 0.75 shifts -> unlikely new person will be hired
        """

        cap = capacity
        shifts = shifts
        ash = ash
        year = 1978
        
        # price deflation
        deflate_price = [
                         Tech.index_mult("CE INDEX", year), 
                         Tech.index_mult("CE INDEX", year), 
                         Tech.index_mult("Construction Labor", year),
                         Tech.index_mult("Engineering Supervision", year),
                         Tech.index_mult("Construction Labor", year),
                         Tech.index_mult("Equipment", year),
                         Tech.index_mult("CE INDEX", year),
                        ]
        
        def om(cap, shifts, ash, hload, count = 1):
            """Assume 18% similar to times model -> VT_EPA"""
            
            return 0.18*(25554*hload + 134363) * deflate_price[5] * count

        def om1(cap, shifts, hload, ash, HV = 20.739, count = 1): 

            u_c = cap / 0.60 * (hload / (0.00001105 * hload + 0.0003690)) * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3
            a_d = cap / 0.60 * 700 * hload * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3 * 0.38
            d_l = (shifts*2190)/8760 * (38020 * math.log(hload) + 28640) * 30.62/23.32 #bls divided by inflated labor price in document
            s = (shifts*2190)/8760 * ((hload+5300000)/(99.29-hload)) * 30.62/23.32
            m = (shifts*2190)/8760 * ((hload+4955000)/(99.23-hload)) * 30.62/23.32
            r_p = (1.705 * 10 ** 8 *hload - 2.959 * 10 ** 8)**0.5 * (11800/(HV*500)) ** 1.0028 
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) 
            
            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om2(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (29303 + 719.8 * hload)
            a_d = cap * 0.38 * (547320 + 66038 * math.log(ash/(HV*500))) * (hload/150) ** 0.9754
            d_l = (shifts*2190)/8760 * (202825 + 5.366*hload**2) * 0.539 * 30.62/23.32
            s = (shifts*2190)/8760 * 136900 * 0.539 * 30.62/23.32
            m = (shifts*2190)/8760 * (107003 + 1.873* hload**2) * 0.539 * 30.62/23.32
            r_p = (50000 + 1000 * hload) * 0.539
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) * 0.539  

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om3(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (189430 + 1476.7 * hload)
            a_d = cap*(-641.08 + 70679828*ash/(HV*500))*(hload/200) ** 1.001 * 0.38
            d_l = (shifts*2190)/8760 * (244455 + 1157 * hload) * 0.539 * 30.62/23.32
            s = (shifts*2190)/8760 * (243895 - 20636709/hload) * 0.539 * 30.62/23.32
            m = (shifts*2190)/8760 *(-1162910 + 256604 * math.log(hload)) * 0.539 * 30.62/23.32
            r_p = (180429 + 405.4 * hload) * 0.539
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) * 0.539  

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om4(cap, shifts, hload, ash, HV = 28.608, count = 1):
      
            u_c = cap/0.45 * (580 * hload + 3900)
            d_l = (shifts*2190)/8760 * 105300 * 30.62/23.32

            if hload < 15:
                s = (shifts*2190)/8760 * (hload - 5)/10 * 68500 * 30.62/23.32
                m = (shifts*2190)/8760 * (1600 * hload + 8000) * 30.62/23.32
            else:
                s = (shifts*2190)/8760 * 68500 * 30.62/23.32
                m = (shifts*2190)/8760 * 32000 * 30.62/23.32
            r_p = 708.7 * hload + 4424 
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) 
    
            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        def om5(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap/0.55 * (202 * hload + 24262)
            d_l = (shifts*2190)/8760 * (hload**2/(0.0008135 * hload - 0.01585)) * 30.62/23.32
            s = (shifts*2190)/8760 * 68500 * 30.62/23.32
            m = (shifts*2190)/8760 * (-1267000/hload + 77190) * 30.62/23.32
            r_p = 7185 * hload ** 0.4241
            o = (0.3 * d_l + 0.26 * (d_l + s + r_p + m)) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        def om6(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (43671.7 + 479.6 * hload)
            d_l = (shifts*2190)/8760 * (173197 + 734 * hload) * 0.539 * 30.62/23.32
            s = (shifts*2190)/8760 * (263250 - 30940000 / hload) * 0.539 * 30.62/23.32
            m = (shifts*2190)/8760 * (32029 + 320.4 * hload) * 0.539 * 30.62/23.32
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
        self.om_val = cost
        self.elec_gen = 0 
        eff_map = self.get_efficiency()
        self.fc = (3600 * sum([a/b for a,b in zip(self.load_8760, eff_map)]) / self.hv_vals[self.fuel_type])
        self.decomm = 0
        self.em_costs = self.get_emission_cost(eff_map)
        
    def capital(self):
        
        """ capital cost models do not include contingencies, land, working capital"""
        
        year = 1978

        deflate_capital = [Tech.index_mult("Equipment", year), Tech.index_mult("Equipment", year), 
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
            
            equip = 15981 * hload ** 0.561
            inst = 4261 * hload + 56041
            i_c = 2256 * hload + 28649
            
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
        ng_cap = OrderedDict({'A': cap, 'B': cap4, 'C': cap5, 'D': cap6})
        petro_cap = OrderedDict({'A': cap, 'B': cap4, 'C': cap5, 'D': cap6})

        boiler_cap = {'COAL': coal_cap, 'NG': ng_cap, 'PETRO': petro_cap}
        
        cost_cap = 0
        for i in range(len(self.boiler_list[0])):
            cost_cap += list(boiler_cap[self.fuel_type].values())[self.boiler_list[0][i][0]] \
                        (hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])
       
        def la():
        #year = 2013
        # add in the appropriate deflation
        
            def calc_area(hload, count = 1):
                """ see the csv file with cleaver brooks/babcock boilers. Returns a value in quarter acre"""
                return (1.68 *hload - 26.1) * count / 1011.714

            total_land_area = 0
            
            for i in range(len(self.boiler_list[0])):
                total_land_area += calc_area(hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])
            
            return total_land_area
        
        self.landarea = la()
        
        eff_cost = 0

        if self.economizer:
            eff_cost += 0

        # add 20% for contingencies as suggested by EPA report
        self.cap_val = cost_cap * 1.2 + self.landarea*self.valperacre + eff_cost

class CHP(Tech):
    def __init__(self,county, hload, fuel):
        """
        https://www.energy.gov/sites/prod/files/2016/04/f30/CHP%20Technical%20Potential%20Study%203-31-2016%20Final.pdf
        size to thermal load
        
        emissions - https://www.energy.gov/sites/prod/files/2016/09/f33/CHP-Steam%20Turbine.pdf
        https://www.energy.gov/sites/prod/files/2016/09/f33/CHP-Gas%20Turbine.pdf
        
        https://www.faberburner.com/resource-center/conversion-charts/emissions-conversions-calculator/
        emission conversions
        
        """
        Tech.__init__(self, county, hload, fuel)
        # Not using sys 2 for energy model in excel file
        self.chp_t = "gas"
        # MW Load - select within model for now -is this factor really needed with 8760 data?
        self.avail_fac = 0.9
        self.dep_year = 5
        self.tech_type = "CHP"
        # process csv file from data request. Use incidence rate as likelihood 
        self.incrate = 0.1
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
        self.elec_gen = -1 * self.avail_fac * [self.char["net_ep"](self.peak_load) * i/self.peak_load for i in self.load_8760]   

        # since p2h ratio same, just multiply by useful power initially
        net_ep = sum(self.elec_gen)
        self.om_val = self.avail_fac * self.char["i_om"](self.peak_load) * net_ep * Tech.index_mult("Equipment", year)
        
        self.decomm = self.char["i_decomm"](self.peak_load)
        
        fuel_input = self.char["fuel_input"](self.peak_load)
        self.fc = self.avail_fac * (3600 * sum([fuel_input * eff for eff in eff_map]) /  self.hv_vals[self.fuel_type])

        self.em_costs = self.get_emission_cost(eff_map)
        
    def capital(self):
        year = 2017
        deflate_capital = [Tech.index_mult("Equipment", year), Tech.index_mult("Equipment", year), Tech.index_mult("CE INDEX", year)]
        ctg = 0.2 #contingency
        i_equip = self.char["i_equip"](self.peak_load)
        i_install = self.char["i_install"](self.peak_load)
        i_other = self.char["i_other"](self.peak_load)
        
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
            return (-b + math.sqrt(b**2 - 4*a*c))/(2*a)

        if self.chp_t == "steam":
            
            load_replaced = [0.6 for x in load_replaced if x > 0.6 ]
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
            
            return roots[mask][0]


class TechFactory():
    @staticmethod
    def create_tech(form,county,hload,fuel):
        try:
            if form.upper() == "BOILER":
                return Boiler(county,hload,fuel)
            if form.upper() == "PVHP":
                return PVHP(county,hload,fuel)
            if form.upper() == "CHP":
                return CHP(county,hload,fuel)
            if form.upper() == "PTC":
                return PTC(county,hload,fuel)
            if form.upper() == "PTCTES":
                return PTCTES(county,hload,fuel)
            if form.upper() == "DSGLF":
                return DSGLF(county,hload,fuel)
            if form.upper() == "SWH":
                return SWH(county,hload,fuel)
            if form.upper() == "PVEB":
                return PVEB(county,hload,fuel)
            if form.upper() == "PVRH":
                return PVRH(county,hload,fuel)
            if form.upper() == "PVWHR":
                return PVWHR(county,hload,fuel)
# =============================================================================
#             if re.search("EXTENSION",format):
#                 return Extension(format)
#             if re.search("REPLACE",format):
#                 return Replace(format)
# =============================================================================
            raise AssertionError("No Such Technology")
        except AssertionError as e:
            print(e)
            
if __name__ == "__main__":
    pass
