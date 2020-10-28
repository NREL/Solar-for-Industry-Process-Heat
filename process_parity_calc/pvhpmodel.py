# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 17:27:40 2020

@author: wxi
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Apr 6 17:30:47 2020

By vectorizing it took ~20 seconds to run the entire simulation. 
In comparison, for loops in MATLAB take 90 seconds and no vectorization + simple modifications takes 2500 seconds. 
10.5 gbs at peak (after appending all the hourly values) dropping down to 3 gbs at the end
Need to look into either assigning lower bit datatypes or dask like structures
Add function to broadcast variables depending on size 

Storage costs = 2017 usd
PV costs = 2018 USD
HP costs = 2012 Euro


@author: wxi
"""
import numpy as np
import pandas as pd 
import os
import time
import sys

print("This script will use up to 10.5 GBs of memory during hourly simulation.")
stop = input("Enter stop to stop the script. Else, type anything: ")

if stop.lower() == "stop":
    sys.exit()

t0 = time.time()

#Adjust below accordingly
path = "./calculation_data"
PVfilename = "SAM_PV_Model Output_Hourly Data USA.xlsx"


# Load main PV Yield Outputs with geographic information
cities = np.array(['Austin, TX', 'Baton Rouge, LO', 'Denver, CO', 'Lancaster, CA', 'Pittsburg, PA']) 
nSN = len(cities)

# Set Value Ranges
TProRet = np.array([[90, 70], [80, 50], [70, 45]]) # 3 x2. Units of Celsius
nTPR = np.shape(TProRet)[0]

# Load main file with the process load profiles
load = pd.read_csv(os.path.join(path,"load_profile.csv")).to_numpy() 
nLP = np.shape(load)[1] 

LoadSize = np.array([100, 1000, 10000]).reshape(3,1) 
nLPS = len(LoadSize)

PVSizing = np.linspace(0.0025, 0.1, 40).reshape(40,1) 
nPVsize = np.shape(PVSizing)[0]

StoreVolume = np.array(range(0,21)).reshape(1,21)
nSV = np.shape(StoreVolume)[1]

num_sims = nSN * nTPR * nLP * nLPS * nSV * nPVsize 

SAMPVOutput = pd.read_excel(os.path.join(path, PVfilename), sheet_name = None)

#define arrays

TambC = np.array([SAMPVOutput[a]["Tamb (C)"].to_numpy() for a in cities]) # Celsius
Tambmean = np.mean(TambC, axis=1).reshape(nSN,1) # Celsius
Tambmax = np.max(TambC, axis=1).reshape(nSN,1) # Celsius
COPmax = np.array([0.0006652 * a ** 2 - 0.001948 * a * Tambmax + 0.00063526 * Tambmax ** 2 - 0.12737 * a + 0.17914 * Tambmax + 7.80747 
                   for a in TProRet[:,0]]).reshape(nTPR,nSN,1,1)

PVkWInstall = (PVSizing * LoadSize.T)# Units = kWp(elec)/kWh*day 

PeakHeating = COPmax * PVkWInstall.reshape(1,1,nPVsize,nLPS) #Units = kWhth

LPkW = LoadSize * load.T #Units = kWh

StorekWh = StoreVolume.reshape(nSV,1,1,1,1) * PeakHeating.reshape(1,nTPR,nSN,nPVsize,nLPS)# Units = kWh

Storem3 = (StorekWh*3600)/(4190) #Units = m3

for i in range(nTPR):
    Storem3[:,i,:,:,:] = Storem3[:,i,:,:,:] / (TProRet[i,0] - TProRet[i,1])
    
GHI = np.array([SAMPVOutput[a]["GHI (W/m2)"].to_numpy() for a in cities]) # units in W/m2

PV_OutputAC = np.array([SAMPVOutput[a]["AC Energy (MWhe)"].to_numpy() for a in cities])# Units = kWAC/kW_dc installed

#Begin Hourly Simulation here

StorageStatusInit = 0

COPhourly = np.array([0.0006652 * a ** 2 - 0.001948 * a * TambC + 0.00063526 * TambC ** 2 - 0.12737 * a + 0.17914 * TambC + 7.80747 
                   for a in TProRet[:,0]]).reshape(nTPR, nSN, 8760)
                    
PVHPyield = np.broadcast_to(PV_OutputAC, (nTPR,nSN,8760)).reshape(nTPR,nSN,1,1,8760) * COPhourly.reshape(nTPR,nSN,1,1,8760) * np.repeat(PVkWInstall[:,:, np.newaxis], 8760, axis=2).reshape(1,1,nPVsize,nLPS,8760)

# vectorizing simulation arrays
StorageStatus = []      
HeatDump = []
ElecDump = []
LP_Remain = []
Eavail= []
PVHPyield = np.broadcast_to(PVHPyield, (nSV,nTPR,nSN,nPVsize,nLPS, 8760)).astype(np.float32)  # nSV x nTPR x nSN x nPVsize x nLPS x hourly
PVkWInstall = np.broadcast_to(PVkWInstall, (nSV,nTPR,nSN,nPVsize,nLPS)) # nSV x nTPR x nSN x nPVsize x nLPS
PeakHeating = np.broadcast_to(PeakHeating, (nSV,nTPR,nSN,nPVsize,nLPS))  # nSV x nTPR x nSN x nPVsize x nLPS
LPkW = np.broadcast_to(LPkW, (nSV,nTPR,nSN,nPVsize,nLPS, 8760)).astype(np.float32) # nSV x nTPR x nSN x nPVsize x nLPS x hourly

COPhourly = np.repeat(COPhourly[:,:, np.newaxis,:], nPVsize, axis=2)
COPhourly = np.repeat(COPhourly[:,:,:,np.newaxis,:], nLPS, axis = 3)
COPhourly = np.broadcast_to(COPhourly,(nSV,nTPR,nSN,nPVsize,nLPS, 8760)).astype(np.float32)

t0 = time.time()

for i in range(8760):
                                             
    if not i:

        Eavail.append(PVHPyield[:,:,:,:,:,i] + StorageStatusInit) # units = kWh thermal
        
    else:
        
        Eavail.append(PVHPyield[:,:,:,:,:,i] + StorageStatus[i-1])
      
    LP_Remain.append(LPkW[:,:,:,:,:,i] - Eavail[i]) # units = kWh
    
    temp_storage = np.zeros((21,3,5,40,3), dtype = np.float32)
    temphdump = np.zeros((21,3,5,40,3), dtype = np.float32)
    tempedump = np.zeros((21,3,5,40,3), dtype = np.float32)
    
    mask1 = (abs(LP_Remain[i]) <= StorekWh) & (LP_Remain[i] < 0)
    mask2 =(abs(LP_Remain[i]) > StorekWh) & (LP_Remain[i] < 0)
    
    temp_storage[mask1] = abs(LP_Remain[i][mask1])
    temp_storage[mask2] = StorekWh[mask2]
    
    StorageStatus.append(temp_storage)
    
    temphdump[mask2] = abs(LP_Remain[i][mask2]) - StorekWh[mask2] #units = kWh thermal
    tempedump[mask2] = np.divide(temphdump[mask2] , COPhourly[:,:,:,:,:,i][mask2]) # units = kWhe
    
    HeatDump.append(temphdump)
    ElecDump.append(tempedump)
    
    LP_Remain[i][mask2] = 0


#Don't need available energy anymore
del Eavail

#collapsing hourly simulation objects to reduce memory
StorageStatus = np.array(StorageStatus)    
HeatDump = np.array(np.sum(HeatDump, axis = 0))    
ElecDump = np.array(np.sum(ElecDump, axis = 0))
LP_Remain = np.array(np.sum(LP_Remain, axis = 0))  
PVHPyield = np.sum(PVHPyield, axis = 5)
LPkWmax = np.amax(LPkW, axis = 5)
LPkW = np.sum(LPkW, axis = 5)

USDperWattPV = (2.4665*(PVkWInstall) ** -0.054).astype(np.float32)  # USD per watt
PVCapEx = USDperWattPV*PVkWInstall*1000 # USD

USDperkWthHP = 4000 *(PeakHeating) ** -0.558 # USD/kW installed -
HPCapEx = USDperkWthHP*PeakHeating # USD

#
USDperm3Store = Storem3
USDperm3Store[Storem3 > 400] = 11680*np.float_power(Storem3[Storem3 > 400], -0.5545) + 130

USDperm3Store[(Storem3 > 0) & (Storem3 <= 400)] = 403.5*np.float_power(Storem3[(Storem3 > 0) & (Storem3 <= 400)], -0.4676) + 750 # USD/m3

StorageCapEx = USDperm3Store * Storem3;
    
Labor_Costs = 0.60 # Percent of CapEx - Can be regionally adjusted
    
CapEx_PVHP = (1+Labor_Costs)*(PVCapEx + HPCapEx+StorageCapEx); # In USD
    
SpecCapExPVHP = CapEx_PVHP/PeakHeating # ($/kW)

# Insert Calculation of LCOH Based on above
# project cost, "sold" energy to process, and main
# financial input parameters, Carbon taxes/credits,
# etc to show lower LCOH
    
    
years = 20;                       

UsedPVHPHeat = (PVHPyield - HeatDump - StorageStatus[8759]) # % in kWh
etaBoiler = 0.8;
NGOffset = UsedPVHPHeat/etaBoiler;
NGemissons = 0.228; # tons CO2e per MWh
CO2emissions = NGemissons*NGOffset/1000; # In Tons
    
# Price of Carbon
CarbonCost = 100; # $/ton
CarbonSavings = CarbonCost*CO2emissions  # in USD

#removed cost of carbon from LCOH
    
LCOH = (CapEx_PVHP)/(UsedPVHPHeat*years) # simple LCOH, To be Updated!


Financial_Results = pd.DataFrame({"LCOH ($/kWh)" : LCOH.flatten(), 
                                  "CapEx ($)" : CapEx_PVHP.flatten(), 
                                  "Spec CapEx ($/kWth)" : SpecCapExPVHP.flatten(), 
                                  "CO2 Emissions Saved (Tons)" : CO2emissions.flatten() , 
                                  "Carbon Costs ($/Ton)" : [CarbonCost] * num_sims, 
                                  "Carbon Savings ($)" : CarbonSavings.flatten()}, 
                                  index = list(range(1,1+num_sims)))

    
# Calculate Main Parameters
    
SolarFraction = UsedPVHPHeat/LPkW;
UsedHeat_Fraction = UsedPVHPHeat/PVHPyield

#Adjust PV_OutputAC
PV_OutputAC = np.tile(np.sum(PV_OutputAC, axis=1, dtype = np.float32), nSV*nTPR*nPVsize*nLPS).reshape(nSV,nTPR,nSN,nPVsize,nLPS)

AnnualCOP = (PVHPyield - HeatDump -StorageStatus[8759])/(PV_OutputAC * PVkWInstall) # Remove remaining energy in tank
   

# Building Result Matrices
GHI = np.sum(GHI, axis = 1)/1000

#Expand GHI, Tambmean and Tambmax then flatten for dataframe
GHI = np.repeat(GHI[:,np.newaxis], nPVsize, axis = 1)
GHI = np.repeat(GHI[:,:,np.newaxis], nLPS, axis=2)
GHI = np.broadcast_to(GHI,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Tambmean = np.repeat(Tambmean.flatten()[:, np.newaxis], nPVsize, axis = 1 )
Tambmean = np.repeat(Tambmean[:,:,np.newaxis], nLPS, axis=2)
Tambmean = np.broadcast_to(Tambmean,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Tambmax = np.repeat(Tambmax.flatten()[:, np.newaxis], nPVsize, axis = 1 )
Tambmax = np.repeat(Tambmax[:,:,np.newaxis], nLPS, axis=2)
Tambmax = np.broadcast_to(Tambmax,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Main_Meteo = pd.DataFrame({"GHI (kWh/a)" : GHI,
                           "T amb mean ( C)" : Tambmean, 
                           "T amb  max ( C)" : Tambmax}, 
                           index = list(range(1,1+num_sims)))

    
a = np.repeat(cities[:, np.newaxis], nPVsize, axis = 1 )
a = np.repeat(a[:,:,np.newaxis], nLPS, axis=2)
a = np.broadcast_to(a,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Tpro = np.array([[TProRet[:,0][i]]*600 for i in range(len(TProRet[:,0]))]).reshape(nTPR,nSN,nPVsize,nLPS)
Tpro = np.broadcast_to(Tpro,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Tret = np.array([[TProRet[:,1][i]]*600 for i in range(len(TProRet[:,1]))]).reshape(nTPR,nSN,nPVsize,nLPS)
Tret = np.broadcast_to(Tret,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

c = np.ones((nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

LS = np.broadcast_to(LoadSize.flatten(),(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

SV = np.array([[StoreVolume[0,:][i]]*1800 for i in range(len(StoreVolume[0,:]))]).reshape(nSV,nTPR,nSN,nPVsize,nLPS).flatten()

PVkW = np.repeat(PVSizing.flatten()[:, np.newaxis], nLPS, axis = 1)
PVkW = np.broadcast_to(PVkW,(nSV,nTPR,nSN,nPVsize,nLPS)).flatten()

Main_Inputs = pd.DataFrame({"Location": a, "T Process (C)": Tpro, "T return (C)": Tret, "Load Profile Type": c, "Load Size (kWh*day)": LS,
                           "Storage Size (kWh/kWp)": SV, "Storage (kWh)": StorekWh.flatten(), "Storage (m3)": Storem3.flatten(), "PVkW Scale (kW/kWp Process)": PVkW,
                           " PVkW Installed (kWel)": PVkWInstall.flatten(), "PVHP Design Output (kWth)": PeakHeating.flatten(),
                           "Load Peak (kW)": LPkWmax.flatten(), "Average Load (kW)" : LPkW.flatten()/8760}, index = list(range(1,1+num_sims)))


Main_Results = pd.DataFrame({"PV Yield (kWhe)": PV_OutputAC.flatten(), "PVHP Yield(kWhth)": PVHPyield.flatten(), "Process Demand (kWh)": LPkW.flatten(), 
                             "Used PVHP Heat (kWhth)": UsedPVHPHeat.flatten(), "Heat Dump (kWh)": HeatDump.flatten(), "Elec Dump (kWh)": ElecDump.flatten(), 
                             "Solar Fraction": SolarFraction.flatten(), "Annual COP": AnnualCOP.flatten(), 
                             "Used PVHP Heat ": UsedHeat_Fraction.flatten()}, index = list(range(1,1+num_sims)))


Summary_Matrix = pd.concat([Main_Inputs, Main_Meteo, Main_Results, Financial_Results], axis = 1)

#Summary_Matrix.to_csv("./calculation_data/pvhp_sim.csv")

t1 = time.time()
print("The script run time is: ", t1-t0)

