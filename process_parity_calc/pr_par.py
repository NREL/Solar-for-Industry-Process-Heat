"""
List of things that need to be updated year by year:
corporate tax dataset, subsidies from DSire
MACRS depreciation schedule
manually assign discount rate
update the chemical engineering cost index
update heat content using https://www.eia.gov/totalenergy/data/monthly/#appendices spreadsheets
manually update fuel escalation rates using EERC until I figure out how to automate
"""

import Create_LCOH_Obj as CLO
from create_format import FormatMaker
from bisection_method import bisection
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class PrPar:
    
    def __init__(self, new_obj = True):
        """ solar_tech, comb_tech should be appropriate factory objects"""
        
        if new_obj:
        
            print("\nCreating solar object")
            self.s_form = FormatMaker().create_format()
            self.solar = CLO.LCOHFactory().create_LCOH(self.s_form)
                
            print("Creating combustion object")  
            self.c_form = FormatMaker().create_format()
            self.comb = CLO.LCOHFactory().create_LCOH(self.c_form)
            
                
            self.solar_current = self.solar.calculate_LCOH()
            self.comb_current = self.comb.calculate_LCOH()
            
            
        else:
            self.solar = CLO.LCOHFactory().create_LCOH(self.s_form)
            self.comb = CLO.LCOHFactory().create_LCOH(self.c_form)
            self.solar_current = self.solar.calculate_LCOH()
            self.comb_current = self.comb.calculate_LCOH()
            
            
    def reset(self):
        self.__init__(False)
        
    def mc_analysis(self):
        
        mc_solar = self.solar.simulate(no_sims = 100)
        mc_comb = self.comb.simulate(no_sims = 100)
        
        self.reset()
        
    def pp_1D(self, iter_name):
        
        fuel_units = {"NG" : "$/thousand cuf", "PETRO" : "$/gallon", "COAL" : "$/short ton"}
        
        if iter_name.upper() == "INVESTMENT":
            self.solar.iter_name = "INVESTMENT"
            root = bisection(lambda x: self.solar.iterLCOH(x) - self.comb_current, 10 **6, 10 ** 8, 100)
            print("The investment price for process parity is {:.2f} $USD".format(root))
        
        elif iter_name.upper() == "FUELPRICE":
            self.comb.iter_name = "FUELPRICE"
            root = bisection(lambda x: self.comb.iterLCOH(x) - self.solar_current, 0, 20, 100)
           
            print("The fuel price of {} for process parity is {:.2f} {}".format(self.comb.fuel_type, root, fuel_units[self.comb.fuel_type]))

        else:
            print("Not a valid iteration name.")
            return None
        
        self.reset()
        
        return root
        
    def pp_nD(self, tol = 10, no_val = 1000):
        
        """Solution Space"""
    
        self.solar.iter_name = "INVESTMENT"
        self.comb.iter_name = "FUELPRICE"
        
        i_vals = np.linspace(0,10 ** 8, no_val)            
        
        df_sol = pd.DataFrame(columns = ["fuelprice", "investment", "LCOH"])
        
        for i in i_vals:
            root = bisection(lambda x: self.comb.iterLCOH(x) - self.solar.iterLCOH(i), -10, 30, 100)
            df_sol = df_sol.append({"fuelprice": root, "investment" : i}, ignore_index = True)

        plt.scatter(df_sol["fuelprice"], df_sol["investment"])
        plt.title("Solution Space")
        plt.xlabel("Fuel Price (c/kWh)")
        plt.ylabel("Investment Price (USD)")
        
        self.reset()
        
        return df_sol

if __name__ == "__main__":
    test_obj = PrPar()
    test_obj.pp_1D("FUELPRICE")    


        