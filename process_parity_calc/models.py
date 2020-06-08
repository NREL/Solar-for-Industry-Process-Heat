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

class Tech(metaclass=ABCMeta):
    path = "./calculation_data"
    cost_index = pd.read_csv(os.path.join(path, "cost_index_data.csv"), index_col = 0)
    land_price = pd.read_csv(os.path.join(path, "landprices.csv"), index_col = 0)
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

        
class PVHP(Tech):
    """
       Steven's PVHP model
    """
    def __init__(self, county, hload, fuel):
        
        Tech.__init__(self, county, hload, fuel)        
        # Simulation File and Cost Index File
        self.simr = pd.read_csv(os.path.join(Tech.path,"pvhp_sim.csv"))

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
        
    def om(self):
        
        year = 2018
        deflate_price = [Tech.index_mult("Engineering Supervision", year)] 
        
        # Direct labor costs
        d_l = 0.6/1.6 * self.simr.loc[self.lcohid, "CapEx ($)"]
        
        # O&M, Fuel, Electricity Costs, Decomm Costs
        self.om_val = sum(a * b for a, b in zip(deflate_price, [d_l])) * self.lcohcount
        self.fc = 0
        self.ec = 0
        self.decomm = 0

    def capital(self):
        
        year = 2018
        deflate_capital = [Tech.index_mult("Equipment", year)]     
        
        # capital costs
        equip = self.simr.loc[self.lcohid, "CapEx ($)"]
        
        # contingency
        ctg = 0.2
        
        # capital value
        self.cap_val = (1+ctg) * sum(a * b for a, b in zip(deflate_capital, [equip])) * self.lcohcount
        
    def get_efficiency(self):

        """
        PVHP thermal efficiency 
        
        """
        # returns 0 because assume that Solar doesn't use fuel
        
        return 1
    
        
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
        
        under 15 M BTU or 5 M BTU/hr https://www.osti.gov/servlets/purl/797810
        the numbers appear accurate for purchased equipment cost- 
        
        https://iea-etsap.org/docs/TIMES_Dispatching_Documentation.pdf - multiple unit
        
        <5 MMBTU - installed cost Table page 61
        https://www.osti.gov/servlets/purl/797810
    """
    
    def __init__(self,county, hload,fuel):
        
        Tech.__init__(self, county, hload, fuel)
        
        """ Temp in Kelvins, heat load in kW """
        
        # Imported values
        self.eff_dict = {"NG": [70,75], "COAL": [75,85], "PETRO": [72,80]}
        
        # Format String processing
        self.design_load = self.peak_load * 0.00341 / (self.eff_dict[self.fuel_type][1]/100) #specific to boilers
        self.dep_year = 15

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
        self.ec = 0 
        self.fc = (3600 * sum([a/b for a,b in zip(self.load_8760, self.get_efficiency())]) / self.hv_vals[self.fuel_type])
        self.decomm = 0
        
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

        # add 20% for contingencies as suggested by EPA report
        self.cap_val = cost_cap * 1.2 + self.landarea*self.valperacre

class CHP(Tech):
    def __init__(self,county, hload, fuel):
        """
        https://www.energy.gov/sites/prod/files/2016/04/f30/CHP%20Technical%20Potential%20Study%203-31-2016%20Final.pdf
        size to thermal load
        
        """
        Tech.__init__(self, county, hload, fuel)
        # Not using sys 2 for energy model in excel file
        self.chp_t = "gas"
        # MW Load - select within model for now
        self.avail_fac = 0.9
        self.dep_year = 5

        def get_params(hload):
            # make linear extrapolation assumption
               
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

            model = LinearRegression(fit_intercept = True)   
            # update each equation with an interpolation          
            for i in self.char.keys():
                temp_model = model.fit(self.useful_t.flatten().reshape(-1,1), self.char[i].flatten().reshape(-1,1))
                coeff = float(temp_model.coef_)
                intercept = float(temp_model.intercept_)
                self.char[i] = lambda x, coeff = coeff, intercept = intercept: coeff * x  + intercept  
        
        get_params(self.peak_load)
        
    def om(self):
        #define fuel eff as a func of load_replaced
        year = 2017
        load_replaced = [(self.peak_load - i)/self.peak_load for i in self.load_8760]
        eff_map = list(map(self.get_efficiency, load_replaced))
        
        # since p2h ratio same, just multiply by useful power initially
        net_ep = sum([self.char["net_ep"](self.peak_load) * i/self.peak_load for i in self.load_8760])
        self.om_val = self.avail_fac * self.char["i_om"](self.peak_load) * net_ep * Tech.index_mult("Equipment", year)
        
        self.decomm = self.char["i_decomm"](self.peak_load)

        # removing count for now -this unit is in kwh so cost needs to be per kwh
        self.ec = -1 * self.avail_fac * net_ep
        
        fuel_input = self.char["fuel_input"](self.peak_load)
        self.fc = self.avail_fac * (3600 * sum([fuel_input * eff for eff in eff_map]) /  self.hv_vals[self.fuel_type])
    
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
# =============================================================================
#     def check_boiler_choice():
#         
#         ngp_sol = [[(0, 2.0)], [(1, 5.0)], [(1, 20.0)], [(2, 30)], [(2, 30)],
#                    [(2, 70.0)], [(2, 150.0)], [(2, 175.0)], [(3, 200.0)], 
#                    [(3, 250.0)], [(3, 700.0)], [(-1, 700), (0, 2.0)], 
#                    [(-1, 700), (2, 175.0)], [(-1, 700), (2, 30.0)]]|
#         
#         ngp_test = []
#         
#         for i in [a / 0.00341 * 0.75 for a in [2, 5, 20, 29.5, 30, 70,
#                                                       150, 175, 200, 250, 700,
#                                                       702, 875, 1430]] :
#             ng_select = TechFactory.create_tech("BOILER", (i,1), (1, "NG"))
#             ngp_test.append(ng_select.boiler_list[0])
# 
#         assert(ngp_test == ngp_sol), "ngp boiler selection has a problem." 
#         
#         
#         coal_sol = [[(0, 15)], [(0, 15.0)], [(0, 40.5)], [(0, 60.0)],
#                     [(0, 68.0)], [(1, 75.0)], [(1, 150.0)], [(1, 199.0)], 
#                     [(2, 200.0)], [(2, 251.0)], [(2, 700.0)], 
#                     [(-1, 700), (0, 15)], [(-1, 700), (0, 15)]]
#         
#         coal_test = []
#         
#         for i in [a / 0.00341 * 0.85 for a in [10, 15, 40.5, 60, 68, 75,
#                                                       150, 199, 200, 251, 700,
#                                                       710, 1410]] :
#             coal_select = TechFactory.create_tech("BOILER", (i,1), (1, "COAL"))
#             coal_test.append(coal_select.boiler_list[0])
# 
#         assert(coal_test == coal_sol), "coal boiler selection has a problem." 
#         
#         print("Boiler Selection Working")
#     
#     check_boiler_choice()
#     
#     def check_PVHP():
#         for i in [200,1000,3000,5000,7000,9000,11000,13000,15000]:
#             pvhp_select = TechFactory.create_tech("PV+HP", (i,1), (1, "NG"))
#             print(pvhp_select.lcohcount)
#     check_PVHP()
# =============================================================================
    # need to make sure numbers are int cast for assertion test in case python
    # rounding errors