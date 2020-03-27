# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 16:57:50 2020
Contains all the OM/capital cost as a function of temp bin, tech type, 
load bin - can just be a spreadsheet mapping stuff
@author: wxi

"""

import os
from abc import ABCMeta, abstractmethod
import pandas as pd
from collections import OrderedDict, namedtuple, Counter
import math


class Tech(metaclass=ABCMeta):
    path = "./calculation_data"
    @abstractmethod
    def __init__(self):

        print("Initialize variables that require user input/format")

    @abstractmethod
    def om(self):

        print("Returns the operating and maintenance costs (fixed+variable)")

    @abstractmethod
    def capital(self):

        print("Returns the capital cost.")

    @abstractmethod
    def index_mult(self):

        print("Returns the appropriate cost_index multiplier.")
        
class Boiler(Tech):
    """
        Using the EPA report from 1978 to populate this boiler model. The EPA
        report has varoius cost curves for different fuel types/loads. 

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
    """
    
    def __init__(self,county,temp,hload,fuel):
        
        """ Temp in Kelvins, heat load in kW """
        self.county = county
        self.temp = temp
        # Q
        self.heat_load = hload*0.00341 # unit conversion to M btu/hr for model -it's the peak heat load
        # fueltype 
        self.fuel_price, self.fuel_type = fuel
        self.cost_index = pd.read_csv(os.path.join(Tech.path, "cost_index_data.csv"), index_col = 0)
        
        def select_boilers():
            
            heat_load = self.heat_load
            
            boiler = namedtuple('Key', ['PorE', 'lower', 'upper', 'WorF'])

            coal_boilers = OrderedDict({
                            'A': boiler("packaged", 15, 60, "watertube"), 
                            'B': boiler("erected", 75, 199, "watertube"), 
                            'C': boiler("erected", 200, 700, "watertube")
                            })

            petro_ng_boilers = OrderedDict({
                                'D': boiler("packaged", 5, 29, "firetube"), 
                                'E': boiler("packaged", 30, 150, "watertube"), 
                                'F': boiler("erected", 200, 700, "watertube"),
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
            
            
    def index_mult(self, index_name, year1, year2 = -1):
            
        try:
                
            return self.cost_index.loc[index_name, self.cost_index.columns[year2]] / self.cost_index.loc[index_name, str(year1)]
            
        except KeyError:
                
            if year1 < int(self.cost_index.columns[1]) or year2 > int(self.cost_index.columns[-1]):
                    
                print("Please pick year between {} to {}".format(
                        self.cost_index.columns[0], self.cost_index.columns[-1]))
            else:
                    
                print("Not a valid cost index.")
                
    def get_efficiency(self):

        """
        
            Efficiency defined by iea technology brief
            by fuel type (full load) - coal: 85% oil: 80% NG:75%
        
        """
        
        eff_dict = {"NG": [70,75], "COAL": [75,85], "PETRO": [72,80]}
        
        # add something to process efficiency as a function of hload if
        # efficiency as a function of load
        
        self.eff =  eff_dict[self.fuel_type][1]
    

    def om(self,capacity = 1, shifts = 4, ash = 6):

        """ all the om equations do not include fuel or any sort of taxes"""

        cap = capacity
        shifts = shifts
        ash = ash
        year = 1978
        
        deflate_price = [
                         self.index_mult("PPI_CHEM", year), 
                         self.index_mult("CE INDEX", year), 
                         self.index_mult("Construction Labor", year),
                         self.index_mult("Engineering Supervision", year),
                         self.index_mult("Construction Labor", year),
                         self.index_mult("Equipment", year),
                         self.index_mult("CE INDEX", year),
                        ]
                
        def om1(cap, shifts, hload, ash, HV = 28.608, count = 1): 

            u_c = cap / 0.60 * (hload / (0.00001105 * hload + 0.0003690)) * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3
            a_d = cap / 0.60 * 700 * hload * (11800/(HV*500)) ** 0.9 * (ash/10.6) ** 0.3 * 0.38
            d_l = (shifts*2190)/8760 * (38020 * math.log(hload) + 28640)
            s = (shifts*2190)/8760 * ((hload+5300000)/(99.29-hload))
            m = (shifts*2190)/8760 * ((hload+4955000)/(99.23-hload))
            r_p = (1.705 * 10 ** 8 *hload - 2.959 * 10 ** 8)**0.5 * (11800/(HV*500)) ** 1.0028
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m)
            
            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om2(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (29303 + 719.8 * hload)
            a_d = cap * 0.38 * (547320 + 66038 * math.log(ash/(HV*500))) * (hload/150) ** 0.9754
            d_l = (shifts*2190)/8760 * (202825 + 5.366*hload**2)
            s = (shifts*2190)/8760 * 136900
            m = (shifts*2190)/8760 * (107003 + 1.873* hload**2)
            r_p = 50000 + 1000 * hload
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m)

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om3(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (189430 + 1476.7 * hload)
            a_d = cap*(-641.08 + 70679828*ash/(HV*500))*(hload/200) ** 1.001 * 0.38
            d_l = (shifts*2190)/8760 * (244455 + 1157 * hload)
            s = (shifts*2190)/8760 * (243895 - 20636709/hload)
            m = (shifts*2190)/8760 *(-1162910 + 256604 * math.log(hload))
            r_p = 180429 + 405.4 * hload
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, a_d, d_l, s, m, r_p, o])) * count

        def om4(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap/0.45 * (580 * hload + 3900)
            d_l = (shifts*2190)/8760 * 105300

            if hload < 15:
                s = (shifts*2190)/8760 * (hload - 5)/10 * 68500
                m = (shifts*2190)/8760 * (1600 * hload + 8000)
            else:
                s = (shifts*2190)/8760 * 68500
                m = (shifts*2190)/8760 * 32000
            r_p = 708.7 * hload + 4424
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        def om5(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap/0.55 * (202 * hload + 24262)
            d_l = (shifts*2190)/8760 * (hload**2/(0.0008135 * hload - 0.01585))
            s = (shifts*2190)/8760 * 68500
            m = (shifts*2190)/8760 * (-1267000/hload + 77190)
            r_p = 7185 * hload ** 0.4241
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        def om6(cap, shifts, hload, ash, HV = 28.608, count = 1):

            u_c = cap * (43671.7 + 479.6 * hload)
            d_l = (shifts*2190)/8760 * (173197 + 734 * hload)
            s = (shifts*2190)/8760 * (263250 - 30940000 / hload)
            m = (shifts*2190)/8760 * (32029 + 320.4 * hload)
            r_p = 50000 + 250 * hload
            o = 0.3 * d_l + 0.26 * (d_l + s + r_p + m) 

            return sum(a * b for a, b in zip(deflate_price, [u_c, 0, d_l, s, m, r_p, o])) * count

        """ for now heating values done manually from Table_A5_...Coal in calculation_data"""
        coal_om = OrderedDict({'A': om1, 'B': om2, 'C': om3})
        ng_om = OrderedDict({'A': om4, 'B': om5, 'C': om6})
        petro_om = OrderedDict({'A': om4, 'B': om5, 'C': om6})

        boiler_om = {'COAL': coal_om, 'NG': ng_om, 'PETRO': petro_om}
        
        cost = 0
        
        for i in range(len(self.boiler_list[0])):
            cost += list(boiler_om[self.fuel_type].values())[self.boiler_list[0][i][0]](
            cap = cap, shifts = shifts, ash = ash, hload = self.boiler_list[0][i][1], count = self.boiler_list[1][i])

        self.om_val = cost
        
    
    def capital(self):
        pass
    

class TechFactory():
    @staticmethod
    def create_tech(form,county,temp,hload,fuel):
        try:
            if form.upper() == "BOILER":
                return Boiler(county,temp,hload,fuel)
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
    
    test = Boiler("1001",300,419354.83871,(1,"NG"))
    test.om()
    print(test.om_val)