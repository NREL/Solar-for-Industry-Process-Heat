#!/usr/bin/env python
# coding: utf-8

# In[1]:


import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pyarrow
import h5py
from Results_techopp import techopp


# -----------------------------------------------------------------------


# files for tech opp analysis

files = techopp().tech_opp_files

swh, lf, ptc_notes, ptc_tes, eboiler, res, whrhp = techopp.read_h5(files, 'summer') #winter


# look at PTC cases as example
ptcNoTES= techopp.h5_to_df(ptc_notes, 'ophours_mean') #'ophours_low'
ptcTES= techopp.h5_to_df(ptc_tes, 'ophours_mean')


time_index = pd.DataFrame(ptc_notes['time_index'], dtype=str)

time_index = pd.to_datetime(time_index[0])



# pick a county that has key industries (311,322,324,325) present and with a high county load
ptcTES[(ptcTES['311']==True) & (ptcTES['322']==True) & (ptcTES['324']==True) & (ptcTES['325']==True) &
      (ptcTES.cty_load>497143)].COUNTY_FIPS.unique()



cty = 19153 #FIPS CODE for Polk Co. Iowa, also tried 53033

techoppHourly = pd.DataFrame(list(ptcNoTES[ptcNoTES.COUNTY_FIPS==cty]['tech_opp'])).T


techoppDay = techoppHourly.set_index(time_index).rename(columns={0:'tech_opp'}).reset_index().rename(columns={0:'time'})

#account for timezone --- DONT NEED TO BC NOT A REV FILE, ALREADY ACCOUNTED FOR IN RESULTS FILES
tz = 0 #(8,7,6,5)

techoppDay.loc[:, 'hour'] = np.tile(range(0, 8760),1)
techoppDay.loc[:,'centralHour'] = np.tile(range(0-tz, 8760-tz),1)
techoppDay.loc[0:(tz-1),'centralHour'] = np.tile(range(8760-tz, 8760),1)


#make a column for days
for day in range(365):
    techoppDay.loc[:,'day'] = techoppDay['centralHour']//24

#re-number the hours from 0-8760 to 0-23 for each day, and add months
topp = techoppDay.copy()

topp.loc[:,'dailyHour'] = topp['centralHour']-(24*topp['day'])+1

topp.loc[:,'month'] = topp['day']//31+1



topp.dailyHour = topp.dailyHour.astype(int)

topp.month = topp.month.astype(int)



monthly_av = topp.groupby(['month','dailyHour'])['tech_opp'].mean().reset_index()

monthly_av = monthly_av.pivot("dailyHour","month","tech_opp")

monthly_av_from5 = monthly_av.reindex([5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,1,2,3,4])


# PLOT solar fraction as a heatmap
plt.figure(figsize = (10,10))

ax = sns.heatmap(monthly_av_from5,
                 cmap='RdYlBu_r',center=0.6,annot=True,fmt=".1f",annot_kws={'size':10},cbar=False)
                 #cbar_kws={'label': 'Solar fraction'}) #annot=True,fmt=".1f",annot_kws={'size':10},
ax.invert_yaxis()

plt.ylabel("Daily hour", fontname='Arial',fontsize=12)
plt.xlabel("Month", fontname='Arial',fontsize=12)




# -----------------------------------------------------------------------

# files for temperature analysis
mfg_eu_temps = pd.read_parquet('mfg_eu_temps_20200826_2224.parquet.gzip', engine='pyarrow')

rev_output = h5py.File('C:/users/carrie schoeneberger/desktop/swh_sc0_t0_or0_d0_gen_2014.h5','r')



# get a single county's min and max process temp
# Bee County, TX

county48025 = mfg_eu_temps[mfg_eu_temps.COUNTY_FIPS==48025]

min_temp = county48025.Temp_C.min()
max_temp = county48025.Temp_C.max()



# get time and temp data from rev output file, make dataframe
#still need to incorporate timezone
time_index = pd.DataFrame(rev_output['time_index'], dtype=str)

time_index = pd.to_datetime(time_index[0])

temp_data = pd.DataFrame(rev_output['T_deliv'], index=time_index)


# Resample hourly as mean of 30-min data
temp_data = temp_data.resample('H').mean()

one_cty = temp_data[:][805]

final_data = pd.DataFrame(one_cty).reset_index().rename(columns={0:'time',805:'T_deliv (C)'})
final_data.loc[:, 'hour'] = np.tile(range(0, 8760),1)
final_data.loc[:,'CT hour'] = np.tile(range(0-6, 8760-6),1)
final_data.loc[0:(6-1),'CT hour'] = np.tile(range(8760-6, 8760),1)


#make a column for days
for day in range(365):
    final_data.loc[:,'day'] = final_data['CT hour']//24

#re-number the hours from 0-8760 to 0-23 for each day, and add months
fd = final_data.copy()

fd.loc[:,'daily hour'] = fd['CT hour']-(24*fd['day'])+1

fd.loc[:,'month'] = fd['day']//31+1

#use Tsolar (for county x) - Tproc,min (for county x) as temperature difference
fd.loc[:,'T_diff'] = fd.loc[:,'T_deliv (C)']-min_temp


fd.groupby(['month','daily hour'])['T_diff'].mean().reset_index()




monthly_av = fd.groupby(['month','daily hour'])['T_diff'].mean().reset_index()
monthly_av = monthly_av.pivot("daily hour","month","T_diff")
#monthly_av_from5 = monthly_av.reindex([5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,1,2,3,4])

#monthly_av = fd.groupby(['day','daily hour'])['T_diff'].mean().reset_index()
#monthly_av = monthly_av.pivot("daily hour","day","T_diff")


# PLOT temperature difference as a heatmap
plt.figure(figsize = (10,8))

ax = sns.heatmap(monthly_av,cmap='RdYlBu_r',center=0,
                 cbar_kws={'label': 'T,solar - T,process min (C)'}) #annot=True,
ax.invert_yaxis()

plt.ylabel('Daily hour',fontname='Arial',fontsize=14)
plt.xlabel('Month',fontname='Arial',fontsize=14)





