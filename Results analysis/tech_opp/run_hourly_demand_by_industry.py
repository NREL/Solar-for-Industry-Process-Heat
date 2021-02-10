#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os

from Results_techopp import techopp

path = "C:/Users/Carrie Schoeneberger/Desktop/ESIF/hourly_process_demand/"
files = os.listdir(path)

path_low = "C:/Users/Carrie Schoeneberger/Desktop/ESIF/hourly_process_demand/low/"
files_low = os.listdir(path_low)


# read in files, create list of the data inside
def create_naics_list(path,filename,list_soltech):
    df = pd.read_parquet(os.path.join(path, f), engine='pyarrow')
    df = df.groupby('county_fips')['MW'].apply(list).reset_index(name='{}'.format(f[0:3])).set_index('county_fips')
    list_soltech.append(df)
    return list_soltech



# create dataframe for the hourly process demand for each solar tech by industry
ls =[]
for f in files:
    if f[4:7]=='ebl':
        ls=create_naics_list(path,f,ls)
ebl_ind = pd.concat(ls, axis=1)



ls =[]
for f in files:
    if f[4:7]=='whr':
        ls=create_naics_list(path,f,ls)
whr_ind = pd.concat(ls, axis=1)



ls =[]
for f in files:
    if f[4:7]=='res':
        ls=create_naics_list(path,f,ls)
res_ind = pd.concat(ls, axis=1)



ls =[]
for f in files:
    if f[4:7]=='swh':
        ls=create_naics_list(path,f,ls)
swh_ind = pd.concat(ls, axis=1)



ls =[]
for f in files:
    if f[4:7]=='lfs':
        ls=create_naics_list(path,f,ls)
lfs_ind = pd.concat(ls, axis=1)



ls =[]
for f in files:
    if f[4:7]=='ptc':
        ls=create_naics_list(path,f,ls)
ptc_ind = pd.concat(ls, axis=1)




# read in tech opp files, change h5 files to dfs

files = techopp().tech_opp_files

swh, lf, ptc_notes, ptc_tes, eboiler, res, whrhp = techopp.read_h5(files, 'summer') #winter

#swh = techopp.h5_to_df(swh,'ophours_mean')
#lf = techopp.h5_to_df(lf, 'ophours_mean')
#ptc_notes= techopp.h5_to_df(ptc_notes, 'ophours_mean')
#ptc_tes= techopp.h5_to_df(ptc_tes, 'ophours_mean')

eboiler= techopp.h5_to_df(eboiler, 'ophours_mean')  #'ophours_low'
res= techopp.h5_to_df(res, 'ophours_mean')
whrhp= techopp.h5_to_df(whrhp, 'ophours_mean')



def get_solar_potential_industry(hrly_ind_demand, techopp_df):
    
    #combine hourly process demand df with the tech_opp and county columns of the tech opp dfs
    ind_techopp = pd.merge(hrly_ind_demand, techopp_df[['COUNTY_FIPS','tech_opp']],
                   left_on='county_fips',right_on='COUNTY_FIPS')
    
    #compare the hourly process heat demand of each industry to the hourly solar fraction (tech_opp),
    # and calculate the amount of solar energy provided (MWh) for the entire year for each 3dig naics industry
    for i in list(ind_techopp.columns[:-2]):  
        for j in list(ind_techopp.index.values):
            if np.isnan(ind_techopp[i][j]).any():
                ind_techopp.loc[j,i] = 'nan'
            else:
                ind_techopp.loc[j,i] = (ind_techopp[i][j] *
                                        np.where(ind_techopp['tech_opp'][j]>=1, 1, ind_techopp['tech_opp'][j])).sum()
    
    ind_techopp_summed = ind_techopp.replace('nan',0).drop(columns=['COUNTY_FIPS','tech_opp']).sum()
    
    return ind_techopp_summed





#swh_ind_sum = get_solar_potential_industry(swh_ind, swh)
#lfs_ind_sum = get_solar_potential_industry(lfs_ind, lf)
#ptc_notes_ind_sum = get_solar_potential_industry(ptc_ind, ptc_notes)
#ptc_tes_ind_sum = get_solar_potential_industry(ptc_ind, ptc_tes)


#swh_ind_sum = swh_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'FPC for SWH'})
#lfs_ind_sum = lfs_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'LF w/ DSG'})
#ptc_notes_ind_sum = ptc_notes_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'PTC w/o TES'})
#ptc_tes_ind_sum = ptc_tes_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'PTC w/ TES'})

#swh_ind_sum.to_csv('swh_potential_industry.csv')
#lfs_ind_sum.to_csv('lfs_potential_industry.csv')
#ptc_notes_ind_sum.to_csv('ptc_notes_potential_industry.csv')
#ptc_tes_ind_sum.to_csv('ptc_tes_potential_industry.csv')




ebl_ind_sum = get_solar_potential_industry(ebl_ind, eboiler)
whr_ind_sum = get_solar_potential_industry(whr_ind, whrhp)
res_ind_sum = get_solar_potential_industry(res_ind, res)


ebl_ind_sum = ebl_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'Eboiler'})
whr_ind_sum = whr_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'WHR HP'})
res_ind_sum = res_ind_sum.reset_index().rename(columns ={'index':'naics_sub',0:'Resistance'})


ebl_ind_sum.to_csv('ebl_potential_industry.csv')
whr_ind_sum.to_csv('whr_potential_industry.csv')
res_ind_sum.to_csv('res_potential_industry.csv')


