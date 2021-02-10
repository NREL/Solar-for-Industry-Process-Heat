#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os

import matplotlib.pyplot as plt
from functools import reduce
import seaborn as sns



#files for solar heat potential

swh = pd.read_csv('swh_potential_industry.csv',index_col=[0])
lfs = pd.read_csv('lfs_potential_industry.csv',index_col=[0])
ptc_notes = pd.read_csv('ptc_notes_potential_industry.csv',index_col=[0])
ptc_tes = pd.read_csv('ptc_tes_potential_industry.csv',index_col=[0])
ebl = pd.read_csv('ebl_potential_industry.csv',index_col=[0])
whr = pd.read_csv('whr_potential_industry.csv',index_col=[0])
res = pd.read_csv('res_potential_industry.csv',index_col=[0])




combined_techs = reduce(lambda x,y: pd.merge(x,y, on='naics_sub', how='outer'),
       [swh,lfs,ptc_notes,ptc_tes,ebl,res,whr]).set_index('naics_sub').sort_index()

combined_techs.rename(columns={'FPC for SWH':"FPC",'LF w/ DSG':"LF",'PTC w/o TES':"PTC no TES"},inplace=True)

#conversion MWh to MMBtu
combined_techs = combined_techs*3.412141

combinedT = combined_techs.T

combinedT.columns = combinedT.columns.astype(str)

combinedT.rename(index={'FPC for SWH':"FPC",'LF w/ DSG':"LF",'PTC w/o TES':"PTC no TES"},inplace=True)




# plot each subsector separately

def create_subsector_plot(subsector,title):
    f, (ax1) = plt.subplots(1, 1, figsize=(6, 5))


    x = combinedT.index.values
    y1 = combinedT[subsector]/(10**6) #MMBtu to TBtu
    sns.barplot(x=x, y=y1, palette="coolwarm", ax=ax1)

    font='Arial'
    ax1.axhline(0, color="k", clip_on=False)
    ax1.set_ylabel("Solar heat (TBtu)",fontname=font,fontsize=14)

    ax1.ticklabel_format(style='scientific',scilimits=[-1, 6], axis='y',useOffset=False)
    
    plt.yticks(fontname=font,fontsize=14)
    plt.xticks(fontname=font,fontsize=14,rotation=60)
    plt.title(label=title,fontname=font,fontsize=16)




create_subsector_plot('311','311, Food')


# ------------------------------------------------
# files for process heat demand


fpc_dem = pd.read_csv('fpc_hw_process_energy.csv.gz',compression='gzip',index_col=[0])
lf_dem = pd.read_csv('LF_process_energy.csv.gz',compression='gzip',index_col=[0])
ptc_dem = pd.read_csv('ptc_process_energy.csv.gz',compression='gzip',index_col=[0])

ebl_dem = pd.read_csv('eboiler_process_energy.csv.gz',compression='gzip',index_col=[0])
whr_dem = pd.read_csv('whr_hp_process_energy.csv.gz',compression='gzip',index_col=[0])
res_dem = pd.read_csv('resistance_process_energy.csv.gz',compression='gzip',index_col=[0])



frac_of_demand = [combined_techs['FPC'].sum()/fpc_dem.hw_MMBtu.sum(),
                  combined_techs['LF'].sum()/lf_dem.MMBtu.sum(),
                  combined_techs['PTC no TES'].sum()/ptc_dem.MMBtu.sum(),
                  combined_techs['PTC w/ TES'].sum()/ptc_dem.MMBtu.sum(), 
                  combined_techs['Eboiler'].sum()/ebl_dem.ebl_MMBtu.sum(),
                  combined_techs['Resistance'].sum()/res_dem.res_MMBtu.sum(),
                 combined_techs['WHR HP'].sum()/whr_dem.whr_MMBtu.sum()]

frac_techs = pd.DataFrame(frac_of_demand,columns=['frac_solar_provided'], index = combined_techs.columns.values)




# plot total solar heat provided and fraction of total heat demand - FOR EACH TECH

f, (ax1) = plt.subplots(1, 1, figsize=(8, 5))

ftsize=12
ftname='Arial'

x1 = frac_techs.index.values
y1 = combined_techs.sum()/(10**6) #MMBtu to TBtu
sns.barplot(x=x1, y=y1, palette="coolwarm", ax=ax1)  #icefire

ax1.axhline(0, color="k", clip_on=False)
ax1.set_ylabel("Solar heat (TBtu)",fontsize=ftsize,fontname=ftname)

ax1.ticklabel_format(style='sci',axis='y',useOffset=False)

#second axis
ax2 = ax1.twinx()

x2 = frac_techs.index.values
y2 = frac_techs['frac_solar_provided']

ax2 = sns.scatterplot(x=x2, y=y2,s=75,color="0.35") 
ax2.set_ylabel('Fraction of total heat demand', fontsize=ftsize,fontname=ftname)

for tick in ax1.get_xticklabels():
    tick.set_fontname(ftname)
    
for tick in ax1.get_yticklabels():
    tick.set_fontname(ftname)
for tick in ax2.get_yticklabels():
    tick.set_fontname(ftname)




