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
from matplotlib.ticker import MaxNLocator
import seaborn as sns
from cashflow import diagram

class PrPar:
    
    def __init__(self, new_obj = True):
        """ solar_tech, comb_tech should be appropriate factory objects"""
        
        if new_obj:
        
            print("\nCreating solar object")
            #self.s_form = FormatMaker().create_format()
            self.s_form = ('GREENFIELD,PVHP', 15000.0, '6085')
            self.solar = CLO.LCOHFactory().create_LCOH(self.s_form)
                
            print("Creating combustion object")  
            #self.c_form = FormatMaker().create_format()
            self.c_form = ('GREENFIELD,BOILER', 15000.0, '6085')
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
        
        mc_solar = self.solar.simulate("MC")
        mc_comb = self.comb.simulate("MC")
        
        # distributions of the two plots
        sns.distplot(mc_solar["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.solar.tech_type)
        sns.distplot(mc_comb["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.comb.tech_type)
        plt.legend()
        
        self.reset()
        
    def to_analysis(self):
        
        to_solar = self.solar.simulate("TO").reset_index(drop = True)
        to_solar.drop(columns = ["Capital Cost", "Operating Cost"], inplace = True)
        val_solar = to_solar["LCOH Value US c/kwh"][0]
        to_solar["LCOH Value US c/kwh"] = (to_solar["LCOH Value US c/kwh"] - val_solar)/val_solar * 100

        to_comb = self.comb.simulate("TO").reset_index(drop = True)
        to_comb.drop(columns = ["Capital Cost", "Operating Cost"], inplace = True)
        val_comb = to_comb["LCOH Value US c/kwh"][0]
        to_comb["LCOH Value US c/kwh"] = (to_comb["LCOH Value US c/kwh"] - val_comb)/val_comb * 100

        fig, ax = plt.subplots(7,1, figsize = (30,20))
        title = fig.suptitle(self.solar.tech_type + " Tornado Diagram", fontsize = 40, fontweight="bold")
        for i in range(7):
            pmask = to_solar.loc[142*i+1:142*i+142, ["LCOH Value US c/kwh"]] > 0
            nmask = to_solar.loc[142*i+1:142*i+142:, ["LCOH Value US c/kwh"]] < 0
            sns.barplot(to_solar[pmask]["LCOH Value US c/kwh"], ax = ax[i], color = "b", ci=None)
            sns.barplot(to_solar[nmask]["LCOH Value US c/kwh"], ax = ax[i], color = "r", ci=None)
            ax[i].tick_params(axis='both', which='major', labelsize=20)
            ax[i].tick_params(axis='both', which='minor', labelsize=8)
            ax[i].set_title(to_solar.columns[i], fontweight="bold", size = 30)
            ax[i].set_xlabel("\u0394LCOH% (c/USD)", size = 22)
            ax[i].set_xlim([-120, 120])
            ax[i].spines['top'].set_visible(False)
            ax[i].spines['right'].set_visible(False)
            ax[i].spines['left'].set_visible(False)
        plt.tight_layout(pad = 3)
        title.set_y(0.95)
        fig.subplots_adjust(top=0.89)
        
        fig, ax = plt.subplots(7,1, figsize = (30,20))
        title = fig.suptitle(self.comb.tech_type + " Tornado Diagram", fontsize = 40, fontweight="bold")
        for i in range(7):
            pmask = to_comb.loc[142*i+1:142*i+142, ["LCOH Value US c/kwh"]] > 0
            nmask = to_comb.loc[142*i+1:142*i+142:, ["LCOH Value US c/kwh"]] < 0
            sns.barplot(to_comb[pmask]["LCOH Value US c/kwh"], ax = ax[i], color = "b", ci=None)
            sns.barplot(to_comb[nmask]["LCOH Value US c/kwh"], ax = ax[i], color = "r", ci=None)
            ax[i].tick_params(axis='both', which='major', labelsize=20)
            ax[i].tick_params(axis='both', which='minor', labelsize=8)
            ax[i].set_title(to_comb.columns[i], fontweight="bold", size = 30)
            ax[i].set_xlabel("\u0394LCOH% (c/USD)", size = 22)
            ax[i].set_xlim([-120, 120])
            ax[i].spines['top'].set_visible(False)
            ax[i].spines['right'].set_visible(False)
            ax[i].spines['left'].set_visible(False)
        plt.tight_layout(pad = 3)
        title.set_y(0.95)
        fig.subplots_adjust(top=0.89)
        
        self.reset()
        
    def pp_1D(self, iter_name):
        
        fuel_units = {"NG" : "$/thousand cuf", "PETRO" : "$/gallon", "COAL" : "$/short ton"}
        
        if iter_name.upper() == "INVESTMENT":
            self.solar.iter_name = "INVESTMENT"
            root = bisection(lambda x: self.solar.iterLCOH(x) - self.comb_current, 0, 10**8, 100)
             
            if root == None:
                print("No solution")
                
            else: 
                print("The investment price for process parity is {:.2f} $USD".format(root))
        
        elif iter_name.upper() == "FUELPRICE":
            self.comb.iter_name = "FUELPRICE"
            root = bisection(lambda x: self.comb.iterLCOH(x) - self.solar_current, 0, 20, 100)
            
            if root == None:
                print("No solution")
                
            else: 
                print("The fuel price of {} for process parity is {:.2f} {}".format(self.comb.fuel_type, root, fuel_units[self.comb.fuel_type]))

        else:
            print("Not a valid iteration name.")
            return None
        
        self.reset()
        
        return root
        
    def pp_nD(self, tol = 10, no_val = 100):
        
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
    
    def pb_irr(self):
        
        energy_yield = sum([self.solar.load_avg * 31536000 / 
                            (1 + self.solar.discount_rate[0]) ** i 
                            for i in range(self.solar.p_time[0])])
        
        value = list(-self.solar_current * energy_yield / (3600*100))
        
        t = np.array([i for i in range(self.solar.year, self.solar.year + self.solar.p_time[0])])
        
        for i in range(1, self.solar.p_time[0]):
            value.append((self.comb.model.fc * self.comb.fuel_price * (1+self.comb.fuel_esc)**i /(1+self.solar.discount_rate[0])**i)[0])

        if np.sum(value[1:]) < -value[0]:
            print("No Payback Period within Lifetime")
            
        fig, ax1 = plt.subplots(figsize=(12,6))
        ax1.set_ylim([-5 * 10 ** 7, 10 ** 7])
    
        title = fig.suptitle("Cash Flow & Payback Period & IRR Diagram", fontweight = "bold", fontsize = 20)
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
    
        ax1.bar(t, value, color = ["#85bb65" if val >= 0 else "#D43E3E" for val in value], alpha = 0.8)
        ax1.set_xlabel("Year", fontsize = 15)
    
        ax1.set_ylabel('Cash Flow $', color = "black", fontsize = 15)
        ax1.tick_params('y', colors = "black")
    
        
        fig.tight_layout()
    
        ax1.spines['top'].set_visible(False)
        ax1.spines['bottom'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['left'].set_visible(False)
        
        title.set_y(0.95)
        fig.subplots_adjust(top=0.85)
        

if __name__ == "__main__":
    test_obj = PrPar()
    test_obj.pb_irr()

