#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import h5py
#import seaborn as sns
import matplotlib.pyplot as plt
import os


from Results_techopp import techopp


"""
Bring in techopp results data; 
choose the data sized for either the summer or winter;
put tech opp results in dataframes for each solar tech
"""

files = techopp().tech_opp_files

swh, lf, ptc_notes, ptc_tes, eboiler, res, whrhp = techopp.read_h5(files, 'summer') #summer or winter


swh = techopp.h5_to_df(swh,'ophours_mean')  # mean or low or high
lf = techopp.h5_to_df(lf,'ophours_mean')
ptc_notes= techopp.h5_to_df(ptc_notes,'ophours_mean')
ptc_tes= techopp.h5_to_df(ptc_tes,'ophours_mean')
eboiler= techopp.h5_to_df(eboiler,'ophours_mean')
res= techopp.h5_to_df(res,'ophours_mean')
whrhp= techopp.h5_to_df(whrhp,'ophours_mean')


#----------------------------------------------------------------------

def get_total_fuels_displaced(swh_df,lf_df,ptc_notes_df,ptc_tes_df,
                              eboiler_df,res_df,whrhp_df,ambhp_noSto_fuels,ambhp_Sto_fuels):
    
    ffs = ['Biomass','Coal','Coke_and_breeze','Diesel','LPG_NGL',
           'Natural_gas','Petroleum_coke','Residual_fuel_oil','Steam',
           'Waste_gas','Waste_oils_tars_waste_materials']
    
    cols_to_keep= ['COUNTY_FIPS','cty_load'] + ffs
    
    
    swh_fuels = swh_df.drop(swh_df.columns.difference(cols_to_keep), axis=1)
    lf_fuels = lf_df.drop(lf_df.columns.difference(cols_to_keep), axis=1)
    ptc_notes_fuels = ptc_notes_df.drop(ptc_notes_df.columns.difference(cols_to_keep), axis=1)
    ptc_tes_fuels = ptc_tes_df.drop(ptc_tes_df.columns.difference(cols_to_keep), axis=1)
    eboiler_fuels = eboiler_df.drop(eboiler_df.columns.difference(cols_to_keep), axis=1)
    res_fuels = res_df.drop(res_df.columns.difference(cols_to_keep), axis=1)
    whrhp_fuels = whrhp_df.drop(whrhp_df.columns.difference(cols_to_keep), axis=1)

    ambhp_noSto_fuels = ambhp_noSto_fuels.drop(ambhp_noSto_fuels.columns.difference(cols_to_keep), axis=1)
    ambhp_Sto_fuels = ambhp_Sto_fuels.drop(ambhp_Sto_fuels.columns.difference(cols_to_keep), axis=1)
    
    
    
    for ff in ffs:
    
        for i in list(swh_fuels.index.values):   
            swh_fuels.loc[i,ff] = swh_fuels[ff][i].sum()
            
        for i in list(lf_fuels.index.values):
            lf_fuels.loc[i,ff] = lf_fuels[ff][i].sum()
         
        for i in list(ptc_notes_fuels.index.values):   
            ptc_notes_fuels.loc[i,ff] = ptc_notes_fuels[ff][i].sum()
            
        for i in list(ptc_tes_fuels.index.values):
            ptc_tes_fuels.loc[i,ff] = ptc_tes_fuels[ff][i].sum()
            
        for i in list(eboiler_fuels.index.values):   
            eboiler_fuels.loc[i,ff] = eboiler_fuels[ff][i].sum()
            
        for i in list(res_fuels.index.values):
            res_fuels.loc[i,ff] = res_fuels[ff][i].sum()
            
        for i in list(whrhp_fuels.index.values):   
            whrhp_fuels.loc[i,ff] = whrhp_fuels[ff][i].sum()
        #    
        for i in list(ambhp_noSto_fuels.index.values):
            ambhp_noSto_fuels.loc[i,ff] = ambhp_noSto_fuels[ff][i].sum()
            
        for i in list(ambhp_Sto_fuels.index.values):
            ambhp_Sto_fuels.loc[i,ff] = ambhp_Sto_fuels[ff][i].sum()
        
        
    conv_factor = 3.412141  #MMBtu/MWh

    
    swhf = dict(zip(ffs, swh_fuels[ffs].sum()*conv_factor))
    lff = dict(zip(ffs, lf_fuels[ffs].sum()*conv_factor))
    ptc_notesf = dict(zip(ffs, ptc_notes_fuels[ffs].sum()*conv_factor))
    ptc_tesf = dict(zip(ffs, ptc_tes_fuels[ffs].sum()*conv_factor))
    eboilerf = dict(zip(ffs, eboiler_fuels[ffs].sum()*conv_factor))
    resf = dict(zip(ffs, res_fuels[ffs].sum()*conv_factor))
    whrhpf = dict(zip(ffs, whrhp_fuels[ffs].sum()*conv_factor))
    
    ambhp_noSto_f = dict(zip(ffs, ambhp_noSto_fuels[ffs].sum()*conv_factor))
    ambhp_Sto_f = dict(zip(ffs, ambhp_Sto_fuels[ffs].sum()*conv_factor))
    
    
    df = pd.DataFrame(data=swhf,index = ['FPC'])
    df = df.append(lff,ignore_index=True)
    df = df.append(ptc_notesf,ignore_index=True)
    df = df.append(ptc_tesf,ignore_index=True)
    df = df.append(eboilerf,ignore_index=True)
    df = df.append(resf,ignore_index=True)
    df = df.append(whrhpf,ignore_index=True)
    df = df.append(ambhp_noSto_f,ignore_index=True)
    df = df.append(ambhp_Sto_f,ignore_index=True).rename(
        index={0:'FPC',1:'LF',2:'PTC no TES',3:'PTC w/ TES',4:'Eboiler',
               5:'Resistance',6:'WHR HP',7:'Amb HP no TES',8:'Amb HP w/ TES'})
    
    return df




allFuels = get_total_fuels_displaced(swh,lf,ptc_notes,ptc_tes,eboiler,res,whrhp,ambhp_noSto_fuels,ambhp_Sto_fuels)


#----------------------------------------------------------------------

# PLOT fuels displaced for each tech by fuel type - stacked bar

allFuelsTBtu = allFuels/(10**6) #MMBtu to TBtu

allFuelsTBtu.rename(columns={'Steam':'Purchased steam'},inplace=True)

allFuelsTBtu.plot(kind='bar',stacked=True,rot=0,cmap='tab20c',fontsize=12, figsize=(10,6))

font="Arial"

plt.yticks(fontname=font,fontsize=13)
plt.xticks(fontname=font,fontsize=14,rotation=60)
plt.ylabel("Fuels displaced (TBtu)",fontname=font, fontsize=14)
plt.legend(loc='upper right',prop={"family":font,"size":12}) #bbox_to_anchor=(1.0, 0.5)
#plt.gca().invert_yaxis()



#----------------------------------------------------------------------

def get_co2_savings(total_fuels_df):
    
    co2EmissFactor = {'Biomass':93.80, 'Coal':94.67, 'Coke_and_breeze':113.67, 'Diesel':74.92, 'LPG_NGL':61.71 ,
                     'Natural_gas':53.06, 'Petroleum_coke':102.41, 'Residual_fuel_oil':72.93,
                     'Waste_gas':12, 'Waste_oils_tars_waste_materials':74.00}
                    # epa.gov/sites/production/files/2018-03/documents/emission-factors_mar_2018_0.pdf
                    # kg CO2/MMBtu
                    # assumptions: Biomass = wood and wood residuals; Coal = Mixed (industrial sector);
                    # Residual fuel oil = No. 5; Waste_oils_tars_waste_materials = Used oil; Diesel = heavy gas oils
    
    co2_df = total_fuels_df.copy()
    co2_df.drop(columns=['Steam'],inplace=True)
    
    for fuel in list(co2_df.columns.values):
        co2_df[fuel] = co2_df[fuel]*co2EmissFactor[fuel]
    
    return co2_df



co2Savings = get_co2_savings(allFuels)




# get sum of annual total CO2 emissions avoided (million metric tons)
co2Savings_totals = co2Savings.sum(axis=1)/(10**9)




# get sum of annual total fuels displaced (TBtu), rounded to one dec. place
allFuelsTBtu = allFuels/(10**6)
allFuelsTBtu.T.sum().round(1)




#----------------------------------------------------------------------

def get_monthly_diff_fuels_displ(soltech_df):
    
    ffs = ['Biomass','Coal','Coke_and_breeze','Diesel','LPG_NGL',
           'Natural_gas','Petroleum_coke','Residual_fuel_oil','Steam',
           'Waste_gas','Waste_oils_tars_waste_materials']
    
    cols_to_keep= ['COUNTY_FIPS','cty_load'] + ffs
    
    fuelsByMonth = soltech_df.drop(soltech_df.columns.difference(cols_to_keep), axis=1)
    
    conv_factor = 3.412141/(10**6) #MMBtu/MWh / MMBtu/TBtu = TBtu/MWh
    
    fuelsByCty = pd.DataFrame()
    for ff in ffs:#['Natural_gas','Coal','Waste_gas','Diesel']
        fuelsByCty.loc[:,ff] = fuelsByMonth[ff].sum()*conv_factor
        
    fuelsByCty.index = fuelsByCty.index+1
    
    return fuelsByCty


swhByCty = get_monthly_diff_fuels_displ(swh)
lfByCty = get_monthly_diff_fuels_displ(lf)
ptc_notesByCty = get_monthly_diff_fuels_displ(ptc_notes)
ptc_tesByCty = get_monthly_diff_fuels_displ(ptc_tes)
eboilerByCty = get_monthly_diff_fuels_displ(eboiler)
resByCty = get_monthly_diff_fuels_displ(res)
whrByCty = get_monthly_diff_fuels_displ(whrhp)




def plot_area_chart(techByCty,chart_title):
    techByCty.rename(columns={'Steam':'Purchased steam'},inplace=True)
    font="Arial"
    techByCty.plot(kind='area',cmap='tab20c')  #Dark2
    plt.xlim(1,12)
    plt.yticks(fontname=font,fontsize=13)
    plt.xticks(fontname=font,fontsize=13)
    plt.ylabel("Fuels displaced (TBtu)",fontname=font, fontsize=14)
    plt.xlabel("Month",fontname=font, fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5),prop={"family":font,"size":13})
    plt.title(chart_title,fontname=font, fontsize=14)
    plt.show()
    
plot_area_chart(ambhp_tes,"Ambient HPs, with storage")





#----------------------------------------------------------------------

# For PV + Ambient HP cases: Back out monthly fuel demand from SWH data
def get_monthly_demand_fuel_type(sol_tech):
    
    ffs = ['Biomass','Coal','Coke_and_breeze','Diesel','LPG_NGL',
           'Natural_gas','Petroleum_coke','Residual_fuel_oil','Steam',
           'Waste_gas','Waste_oils_tars_waste_materials']
    
    cols_to_keep= ['COUNTY_FIPS','total_load'] + ffs

    fuels = sol_tech.drop(sol_tech.columns.difference(cols_to_keep), axis=1)

    fuels['fuels_displ_totals']=fuels.iloc[:,2:].sum(axis=1)

    # 
    mo_index = [744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016]
    for i in list(fuels.index.values):   
        fuels.at[i, 'total_load']=  np.array(list( map(sum, np.array_split(fuels.total_load[i],mo_index) ) ))

        for ff in ffs:
            fuels.loc[i,ff+'_frac']= (fuels[ff][i]/fuels.fuels_displ_totals[i]).mean()
    fuels = fuels.fillna(value=0) 
    for ff in ffs:
        fuels.loc[:,ff] = fuels['total_load']*fuels[ff+'_frac']
        
    return fuels

fuel_demand = get_monthly_demand_fuel_type(swh)




# PV + Ambient HP results, NO storage
ambhp_noStorage = pd.ExcelFile('../tech_opp_tests/calculation_data/Monthly_Loads_Tech_Potential_20201114.xlsx')

ambhp_noSto = pd.read_excel(ambhp_noStorage,'90C Process')

ambhp_noSto.rename(columns={'Unnamed: 0':'COUNTY_FIPS',
                                     'Lost Energy\n(%)':'lost_energy_win',
                                     'Unnamed: 10':'lost_energy_sum'},inplace=True)

ambhp_noSto = ambhp_noSto[['COUNTY_FIPS','lost_energy_win','lost_energy_sum']]

ambhp_noSto.drop(ambhp_noSto.index[0],inplace=True)

# monthly fuel displaced = monthly fuel load * (1-lost energy %)
ambhp_noSto_fuels = pd.merge(ambhp_noSto,fuel_demand,on='COUNTY_FIPS',how='right')

ffs = ['Biomass','Coal','Coke_and_breeze','Diesel','LPG_NGL',
           'Natural_gas','Petroleum_coke','Residual_fuel_oil','Steam',
           'Waste_gas','Waste_oils_tars_waste_materials']

for ff in ffs:  
    ambhp_noSto_fuels.loc[:,ff] = ambhp_noSto_fuels[ff]*(1-ambhp_noSto_fuels['lost_energy_sum'])                    



# PV + Ambient HP, WITH storage
ambhp_Storage = pd.ExcelFile('../tech_opp_tests/calculation_data/Monthly_Loads_Tech_Potential_20200831.xlsx')
ambhp_Sto = pd.read_excel(ambhp_Storage,'90 C Process Temp')
# monthly fuel displaced = monthly fuel load
ambhp_Sto_fuels = fuel_demand



ambhp_notes = get_monthly_diff_fuels_displ(ambhp_noSto_fuels)
ambhp_tes = get_monthly_diff_fuels_displ(ambhp_Sto_fuels)



