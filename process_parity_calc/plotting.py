# -*- coding: utf-8 -*-
"""
Created on Wed Sep 23 09:11:05 2020

@author: wxi
"""

import pandas as pd
import matplotlib.pyplot as plt
import Create_LCOH_Obj as clo
import pr_par as pp
from lcoh_config import ParamMethods as pm

#script for meta data


assert pm.config["mode"] == "su", "set solar sizing mode to su"
 #, "PTCTES","PTC","PVEB"
for county in ['47043', '55071', '18005', '21141', '27079', '29113', '39149', '40047', '48181']:
    print(county)
    for tech in ["PVRH"]:
         model = clo.LCOHFactory().create_LCOH(('REPLACE', tech, False, county))
         model.calculate_LCOH()
         df = pd.DataFrame()
         df["mult"] = model.mult_l
         df["sf"] = model.sf_l
         df["su"] = model.su_l
         lcoh = []
         year0 = []
         fuelpar = []
         investpar = []
         for mult in model.mult_l:
             solarform = [('REPLACE', tech, mult, county)]
             combform = [("GREENFIELD", pm.config["comb"], 0, county)]
             parity = pp.PrPar(form = [solarform,combform])
             lcoh.append(parity.solar_current[0][0])
             year0.append(parity.solar[0].year0[0])
             fuelpar.append(parity.pp_1D("FUELPRICE")[0])
             investpar.append(parity.pp_1D("INVESTMENT")[0])
         df["LCOH (USD Cents/kwh)"] = lcoh
         df["Year0 (USD)"] = year0
         df["Fuel Parity ($/1000 cuf)"] = fuelpar
         df["USD/kwp"] = investpar
         df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + "_.csv")
         print("done")





# =============================================================================
# def plotmeta(path, tech, county):
#     df = pd.read_csv(path)
#     
#     x = df["mult"]
#     data1 = df["sf"]
#     data2 = df["su"]
#     data3 = df["LCOH (USD Cents/kwh)"]
#     data4 = df["Fuel Parity ($/1000 cuf)"]
#     
#     fig, ax1 = plt.subplots()
#     
#     color = 'tab:red'
#     color2 = 'tab:orange'
#     ax1.set_xlabel('Solar Multiplier (1 MW)')
#     ax1.set_ylabel('SF or SU')
#     sf = ax1.plot(x, data1, color = color)
#     su = ax1.plot(x, data2, color = color2)
#     
#     ax2 = ax1.twinx()  
#     
#     color = 'tab:blue'
#     color2 = 'tab:green'
#     ax2.set_ylabel('LCOH (cents/kwh) or Fuel Price')
#     lcoh = ax2.plot(x, data3, color=color, label = "LCOH")
#     fuel = ax2.plot(x, data4, color = color2, label = "Fuel Parity")
#     
#     plots = sf + su + lcoh + fuel
#     ax2.legend(plots, ["Solar Fraction", "Solar Utilization", "LCOH", "Fuel Parity ($/1000 cuf)"], loc = 1)
# 
#     ax2.set_title(" ".join([tech, county, "Sizing"]))
#     fig.tight_layout()
#     
# def plotcounty(tech):
# 
#     
#     fig, ax1 = plt.subplots()
#     
#     for i in ["39049", "6037", "12031", "8059", "8069", "51095", "55079"]:
#         path = "./calculation_data/metaresults/" + "meta_" + i + "_" + tech + "_.csv"
#         df = pd.read_csv(path)
#         data1 = df["sf"]
#         data3 = df["LCOH (USD Cents/kwh)"]
#         data4 = df["Fuel Parity ($/1000 cuf)"]
#         
#         ax1.set_xlabel('Solar Fraction')
#         ax1.set_ylabel('Fuel Price ($/1000 cuf)')
#         sf = ax1.plot(data1, data4, label = i)
#         ax1.legend()
#         
# # =============================================================================
# #         ax2 = ax1.twinx()  
# #         
# #         color = 'tab:blue'
# #         color2 = 'tab:green'
# #         ax2.set_ylabel('Fuel Price')
# #         fuel = ax2.plot(data1, data4, color = color2, label = "Fuel Parity")
# # =============================================================================
#         
#         plots = sf 
# # =============================================================================
# #         ax2.legend(plots, ["LCOH (c/kwh)", "Fuel Parity ($/1000 cuf)"], loc = 1)
# # =============================================================================
#     
#         ax1.set_title(" ".join([tech, "Sizing"]))
#     fig.tight_layout()
# 
# # =============================================================================
# # plotmeta("PVEB_lcohmeta.csv")
# # =============================================================================
# if __name__ == "__main__":
#     path = "./calculation_data/metaresults/"
# 
#     for tech in ["DSGLF", "PTCTES","PTC"]:   
#         plotcounty(tech)
# =============================================================================
