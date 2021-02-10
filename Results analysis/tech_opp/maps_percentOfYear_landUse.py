#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import h5py
#from pandas import DataFrame
import mapclassify as mc
#import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os
import plotly.figure_factory as ff

from Results_techopp import techopp



"""
Bring in techopp results data; 
choose the data sized for either the summer or winter;
put tech opp results in dataframes for each solar tech
"""

files = techopp().tech_opp_files

op_hrs = 'ophours_mean'

swh, lf, ptc_notes, ptc_tes, eboiler, res, whrhp = techopp.read_h5(files, 'summer')


swh = techopp.h5_to_df(swh,op_hrs)
lf = techopp.h5_to_df(lf,op_hrs)
ptc_notes= techopp.h5_to_df(ptc_notes,op_hrs)
ptc_tes= techopp.h5_to_df(ptc_tes,op_hrs)
eboiler= techopp.h5_to_df(eboiler,op_hrs)
res= techopp.h5_to_df(res,op_hrs)
whrhp= techopp.h5_to_df(whrhp,op_hrs)



swh_12, lf_12, ptc_notes_12, ptc_tes_12, eboiler_12, res_12, whrhp_12 = techopp.read_h5(files, 'winter')

swh_12 = techopp.h5_to_df(swh_12,op_hrs)
lf_12 = techopp.h5_to_df(lf_12,op_hrs)
ptc_notes_12= techopp.h5_to_df(ptc_notes_12,op_hrs)
ptc_tes_12= techopp.h5_to_df(ptc_tes_12,op_hrs)
eboiler_12= techopp.h5_to_df(eboiler_12,op_hrs)
res_12= techopp.h5_to_df(res_12,op_hrs)
whrhp_12= techopp.h5_to_df(whrhp_12,op_hrs)



def create_map(tech_opp_df, column_name, title):
    
    map_df = tech_opp_df.copy()
    
    fips = map_df['COUNTY_FIPS'].tolist()
    values = map_df[column_name].tolist() 

    
    endpts = [25,35,50,51]
            #mc.Percentiles(map_df[column_name]).bins.tolist()[1:-1] -- for % of year, all
            #[0.01,0.1,1,10] -- for land use maps
            #[30,40,50,60] -- for % of year, comparing PTC and PTC no tes
            #[35,40,45,50] -- for % of year, comparing PV cases
            #[35,50,75,90] -- for % of year, comparing swh June and Dec
            #[30,35,40,50] -- for % of year, comparing eboiler June and Dec
            
    color = {'reds':["#fef0d9","#fdcc8a","#fc8d59","#e34a33","#b30000"],
             'purples':["#edf8fb","#b3cde3", "#8c96c6","#8856a7","#810f7c"],
            'blues':["#f0f9e8", "#bae4bc","#7bccc4","#43a2ca","#0868ac"]}
    
                #https://colorbrewer2.org/#type=sequential&scheme=BuPu&n=5
    
    
    fig = ff.create_choropleth(
        fips=fips, values=values, scope=['usa'],
        binning_endpoints=endpts, 
        colorscale=color['reds'],
        show_state_data=True,
        state_outline={'color': '#ededed', 'width': 1.0},
        show_hover=True,
        centroid_marker={'opacity': 0.8},
        asp = 2.9,
        #title_text = title,
        #legend_title = title,
        #round_legend_values=True,
        #exponent_format=True,
        county_outline={'color': '#ededed', 'width': 0.5}, #adb7c1 (for blues)  #e0e0e0/ededed (for reds)
    )
    
    fig.layout.template = None
    
    fig.update_layout(legend=dict(
    yanchor="bottom",
    y=0.01,
    xanchor="right",
    x=0.87
    ), legend_title_text=title,
                     font_family="Arial",)
    
    
    fig.show()



create_map(ambhp_ns_land,'land_perc_of_total','Land use, percent of available')




"""
Calculate how often solar fraction >=1 
"""
swh_perc = techopp.get_percent_of_yr(swh)
lf_perc = techopp.get_percent_of_yr(lf)
ptc_notes_perc = techopp.get_percent_of_yr(ptc_notes)
ptc_tes_perc = techopp.get_percent_of_yr(ptc_tes)
eboiler_perc = techopp.get_percent_of_yr(eboiler)
res_perc = techopp.get_percent_of_yr(res)
whrhp_perc = techopp.get_percent_of_yr(whrhp)



swh_12_perc = techopp.get_percent_of_yr(swh_12)
lf_12_perc = techopp.get_percent_of_yr(lf_12)
ptc_12_notes_perc = techopp.get_percent_of_yr(ptc_notes_12)
ptc_12_tes_perc = techopp.get_percent_of_yr(ptc_tes_12)
eboiler_12_perc = techopp.get_percent_of_yr(eboiler_12)
res_12_perc = techopp.get_percent_of_yr(res_12)
whrhp_12_perc = techopp.get_percent_of_yr(whrhp_12)



create_map(whrhp_perc,'Perc_yr','Percent of the year')






"""
Make bar chart of average [land use as percent of total available OR frequency of meeting demand] for each solar tech

"""
column_name = 'land_perc_of_total'
# 'land_perc_of_total' or 'Perc_yr' or'cty_load' (also change data = summed)  

avg = {'Summer sizing': [swh[column_name].mean(),
                       lf[column_name].mean(),
                       ptc_notes[column_name].mean(),
                        ptc_tes[column_name].mean(),
                       eboiler[column_name].mean(),
                       res[column_name].mean(),
                       whrhp[column_name].mean()],
       
       'Winter sizing': [swh_12[column_name].mean(),
                       lf_12[column_name].mean(),
                       ptc_notes_12[column_name].mean(),
                        ptc_tes_12[column_name].mean(),
                       eboiler_12[column_name].mean(),
                       res_12[column_name].mean(),
                       whrhp_12[column_name].mean()]
      }

summed = {'Summer sizing': [swh[column_name].sum(),
                       lf[column_name].sum(),
                       ptc_notes[column_name].sum(),
                            ptc_tes[column_name].sum(),
                       eboiler[column_name].sum(),
                       res[column_name].sum(),
                       whrhp[column_name].sum()],
       
       'Winter sizing': [swh_12[column_name].sum(),
                       lf_12[column_name].sum(),
                       ptc_notes_12[column_name].sum(),
                         ptc_tes_12[column_name].sum(),
                       eboiler_12[column_name].sum(),
                       res_12[column_name].sum(),
                       whrhp_12[column_name].sum()]
      }

#excluding counties where land use > available land
avg_excl = {'Summer sizing': [swh[swh['land_use']<=swh['avail_land']][column_name].mean(),
                                lf[lf['land_use']<=lf['avail_land']][column_name].mean(),
                                ptc_notes[ptc_notes['land_use']<=ptc_notes['avail_land']][column_name].mean(),
                                ptc_tes[ptc_tes['land_use']<=ptc_tes['avail_land']][column_name].mean(),
                                eboiler[eboiler['land_use']<=eboiler['avail_land']][column_name].mean(),
                                res[res['land_use']<=res['avail_land']][column_name].mean(),
                                whrhp[whrhp['land_use']<=whrhp['avail_land']][column_name].mean()
                                ],
       
               'Winter sizing': [swh_12[swh_12['land_use']<=swh_12['avail_land']][column_name].mean(),
                                 lf_12[lf_12['land_use']<=lf_12['avail_land']][column_name].mean(),
                                 ptc_notes_12[ptc_notes_12['land_use']<=ptc_notes_12['avail_land']][column_name].mean(),
                                 ptc_tes_12[ptc_tes_12['land_use']<=ptc_tes_12['avail_land']][column_name].mean(),
                                 eboiler_12[eboiler_12['land_use']<=eboiler_12['avail_land']][column_name].mean(),
                                 res_12[res_12['land_use']<=res_12['avail_land']][column_name].mean(),
                                 whrhp_12[whrhp_12['land_use']<=whrhp_12['avail_land']][column_name].mean()
                                 ]
              }

df_fig = pd.DataFrame(data=avg_excl, 
                      index = ['FPC for SWH','LF with DSG','PTC no TES','PTC with TES',
                               'Electric boiler','Resistance','WHR HPs'])

ax = df_fig.plot(kind='bar',rot=0,color=['darkseagreen','lightsteelblue'],figsize=(12,5))  
#['darkseagreen','lightsteelblue'] ['indianred','lightsteelblue']

font="Arial"
#plt.yticks(fontname=font,fontsize=12)
plt.xticks(fontname=font,fontsize=13)
plt.legend(loc='upper right',prop={"family":font,"size":12})

ax.set_ylabel("Avg. percent of year when solar meets heat demand",fontname=font, fontsize=13)
#Land use, average percent of available  Average percent of year when solar meets heat demand    Total load (MW)


#if plotting with total load for percent of land available 
#ax2 = ax.twinx()
#ax2.scatter(df_fig2.index, df_fig2["Summer sizing"],marker = 'o',c="0.35")
#ax2.set_ylabel("Total load (MW)",fontname=font, fontsize=14)





"""
Determine how much land (km2) is used for each tech

"""
column_name = 'land_use'


summed = {'Summer sizing': [swh[column_name].sum(),
                            lf[column_name].sum(),
                            ptc_notes[column_name].sum(),
                            ptc_tes[column_name].sum(),
                            eboiler[column_name].sum(),
                            res[column_name].sum(),
                            whrhp[column_name].sum()],
       
       'Winter sizing': [swh_12[column_name].sum(),
                         lf_12[column_name].sum(),
                         ptc_notes_12[column_name].sum(),
                         ptc_tes_12[column_name].sum(),
                         eboiler_12[column_name].sum(),
                         res_12[column_name].sum(),
                         whrhp_12[column_name].sum()]
      }

#excluding counties where land use > available land
summed_excl = {'Summer sizing': [swh[swh['land_use']<=swh['avail_land']].land_use.sum(),
                                lf[lf['land_use']<=lf['avail_land']].land_use.sum(),
                                ptc_notes[ptc_notes['land_use']<=ptc_notes['avail_land']].land_use.sum(),
                                ptc_tes[ptc_tes['land_use']<=ptc_tes['avail_land']].land_use.sum(),
                                eboiler[eboiler['land_use']<=eboiler['avail_land']].land_use.sum(),
                                res[res['land_use']<=res['avail_land']].land_use.sum(),
                                whrhp[whrhp['land_use']<=whrhp['avail_land']].land_use.sum()],
       
               'Winter sizing': [swh_12[swh_12['land_use']<=swh_12['avail_land']].land_use.sum(),
                                 lf_12[lf_12['land_use']<=lf_12['avail_land']].land_use.sum(),
                                 ptc_notes_12[ptc_notes_12['land_use']<=ptc_notes_12['avail_land']].land_use.sum(),
                                 ptc_tes_12[ptc_tes_12['land_use']<=ptc_tes_12['avail_land']].land_use.sum(),
                                 eboiler_12[eboiler_12['land_use']<=eboiler_12['avail_land']].land_use.sum(),
                                 res_12[res_12['land_use']<=res_12['avail_land']].land_use.sum(),
                                 whrhp_12[whrhp_12['land_use']<=whrhp_12['avail_land']].land_use.sum()]
              }


land_use_totals = pd.DataFrame(data=summed_excl, 
                      index = ['FPC for SWH','LF with DSG','PTC no TES','PTC with TES',
                               'Electric boiler','Resistance heating','WHR HPs'])

land_use_totals.T.round(0)









"""
Make box plot for [land use (km2),% of land used,% of year] for each tech, winter and summer
"""

import seaborn as sns

col_name = 'Perc_yr'  #land_perc_of_total   Perc_yr


swh6 = swh[[col_name]].assign(sizing = 'Summer', tech = 'SWH')
lf6 = lf[[col_name]].assign(sizing = 'Summer', tech = 'LF')
ptc_notes6 = ptc_notes[[col_name]].assign(sizing = 'Summer', tech = 'PTC no TES')
ptc_tes6 = ptc_tes[[col_name]].assign(sizing = 'Summer', tech = 'PTC with TES')
ebl6 = eboiler[[col_name]].assign(sizing = 'Summer', tech = 'E-boiler')
res6 = res[[col_name]].assign(sizing = 'Summer', tech = 'Resistance')
whr6 = whrhp[[col_name]].assign(sizing = 'Summer', tech = 'WHR HPs')


swh12 = swh_12[[col_name]].assign(sizing = 'Winter', tech = 'SWH')
lf12 = lf_12[[col_name]].assign(sizing = 'Winter', tech = 'LF')
ptc_notes12 = ptc_notes_12[[col_name]].assign(sizing = 'Winter', tech = 'PTC no TES')
ptc_tes12 = ptc_tes_12[[col_name]].assign(sizing = 'Winter', tech = 'PTC with TES')
ebl12 = eboiler_12[[col_name]].assign(sizing = 'Winter', tech = 'E-boiler')
res12 = res_12[[col_name]].assign(sizing = 'Winter', tech = 'Resistance')
whr12 = whrhp_12[[col_name]].assign(sizing = 'Winter', tech = 'WHR HPs')


boxplot_df = pd.concat([swh6,lf6,ptc_notes6,ptc_tes6,ebl6,res6,whr6,
                          swh12,lf12,ptc_notes12,ptc_tes12,ebl12,res12,whr12])


#boxplot_df = boxplot_df[boxplot_df[col_name]<=100]
plt.figure(figsize=(11,6))
ax = sns.boxplot(x="tech", y=col_name, hue="sizing",
                 data=boxplot_df, palette=['indianred','lightsteelblue'], fliersize=1)
                                    # ['darkseagreen','lightsteelblue']  ['indianred','lightsteelblue']

font="Arial"
plt.yticks(fontname=font,fontsize=13)
plt.xticks(fontname=font,fontsize=13)
plt.xlabel(" ")
plt.ylabel("Avg. percent of year when solar meets heat demand",fontname=font, fontsize=13)
#Land use, average percent of available  Avg. percent of year when solar meets demand  

plt.legend(loc='upper right',prop={"family":font,"size":12})




"""
Make summary plot comparing solar techs
"""


lim = 50  #set limit (percent of counties where demand is fully met 'lim'% of the year)

alltechs = {'Perc_yr_solar': [swh['Perc_yr'].mean(),
                       lf['Perc_yr'].mean(),
                       ptc_notes['Perc_yr'].mean(),
                        ptc_tes['Perc_yr'].mean(),
                       eboiler['Perc_yr'].mean(),
                       res['Perc_yr'].mean(),
                       whrhp['Perc_yr'].mean()],
            
           'Perc_counties': [len((swh[swh['Perc_yr']>lim]).index)/len(swh.index)*100,
                       len((lf[lf['Perc_yr']>lim]).index)/len(lf.index)*100,
                       len((ptc_notes[ptc_notes['Perc_yr']>lim]).index)/len(ptc_notes.index)*100,
                        len((ptc_tes[ptc_tes['Perc_yr']>lim]).index)/len(ptc_tes.index)*100,
                       len((eboiler[eboiler['Perc_yr']>lim]).index)/len(eboiler.index)*100,
                       len((res[res['Perc_yr']>lim]).index)/len(res.index)*100,
                       len((whrhp[whrhp['Perc_yr']>lim]).index)/len(whrhp.index)*100],
            
            'Total_load': [swh['cty_load'].sum(),
                       lf['cty_load'].sum(),
                        ptc_notes['cty_load'].sum(),
                        ptc_tes['cty_load'].sum(),
                       eboiler['cty_load'].sum(),
                       res['cty_load'].sum(),
                       whrhp['cty_load'].sum()]
      }

alltechs_df = pd.DataFrame(data=alltechs, index = ['FPC for SWH','LF with DSG','PTC no TES','PTC with TES',
                                   'Electric boiler','Resistance heating','WHR HPs'])




x=alltechs_df['Perc_yr_solar']

y=alltechs_df['Perc_counties']

s=alltechs_df['Total_load']/1e+6
s_round = s.astype(int)

z=  ['FPC','LF','PTC no TES','PTC w/ TES','Eboiler','Res.','WHR HPs']#alltechs_df.index.values

c= ['#2166ac','#67a9cf','#d1e5f0','#DBDBDB','#f4a582','#d6604d','#b2182b']
#['#b2182b', '#d6604d', '#f4a582' ,'#fddbc7', '#d1e5f0', '#67a9cf', '#2166ac']

plt.figure(figsize=(9,6.5))

plt.scatter(x,y,s,c,label=z,
            alpha=0.7, edgecolors="black", linewidth=1)


plt.xlabel("Percent of the year when solar fully meets demand",fontname='Arial',fontsize=14)
plt.ylabel("Percent of counties where solar meets demand >{}% of year".format(lim),fontname='Arial',fontsize=14)
plt.margins(y=0.1)

font="Arial"
plt.yticks(fontname=font,fontsize=14)
plt.xticks(fontname=font,fontsize=14)

labels=[]
for i in range(0,len(c)):
    labels.append(mpatches.Rectangle((0,0),1,1,fc=c[i]))
plt.legend(labels,z,loc=4,prop={"family":'Arial',"size":14})


#for a,b,c in zip(x,y,s_round):
#    plt.annotate(c, # this is the text
#                 (a,b), # this is the point to label
#                 textcoords="offset points", # how to position the text
#                 xytext=(0,30), # distance from text to points (x,y)
#                 ha='center') # horizontal alignment can be left, right or center





"""
Make histograms of land use as percent of total
"""



def make_histogram_land_use(sol_tech,sol_label):
    plt.hist(sol_tech.land_perc_of_total,
             bins=np.logspace(np.log10(0.00001),np.log10(100), 20),
             log=False, alpha=0.5, color='darkseagreen',ec='gray', label=sol_label)
    
    plt.gca().set_xscale("log")
    plt.xlabel("Land use percent of total available", size=14, fontname='Arial')
    plt.ylabel("Count", size=14,fontname='Arial')
    plt.xticks(size=13,fontname='Arial')
    plt.yticks(size=13,fontname='Arial')  #np.arange(0, 550, 100),
    plt.title(sol_label,size=14,fontname='Arial')
    #plt.legend(loc='upper right',prop={"family":'Arial',"size":12})




make_histogram_land_use(eboiler,"E-boiler")





# PV + Ambient HP results, NO storage

ambhp_noStorage = pd.ExcelFile('../tech_opp_tests/calculation_data/Monthly_Loads_Tech_Potential_20201114.xlsx')

ambhp_noStorage_land = pd.read_excel(ambhp_noStorage,'90C Process')

ambhp_noStorage_land.rename(columns={'Unnamed: 0':'COUNTY_FIPS',
                                     'Land Use\n(m2)':'land_use_m2_win',
                                     'Unnamed: 6':'land_use_m2_sum'},inplace=True)

ambhp_noStorage_land = ambhp_noStorage_land[['COUNTY_FIPS','land_use_m2_win','land_use_m2_sum']]

ambhp_noStorage_land.drop(ambhp_noStorage_land.index[0],inplace=True)

ambhp_ns_land = pd.merge(ambhp_noStorage_land,swh[['COUNTY_FIPS','avail_land']],on='COUNTY_FIPS')

ambhp_ns_land = ambhp_ns_land[ambhp_ns_land.avail_land!=0]

ambhp_ns_land.loc[:,'land_perc_of_total'] = ambhp_ns_land['land_use_m2_win']/(1000**2)/ambhp_ns_land['avail_land']*100

ambhp_ns_land.loc[:,'land_use'] = ambhp_ns_land['land_use_m2_win']/(1000**2)

                            

#----------------------------------------

# PV + Ambient HP, WITH storage

ambhp_Storage = pd.ExcelFile('../tech_opp_tests/calculation_data/Monthly_Loads_Tech_Potential_20200831.xlsx')

ambhp_Storage_land = pd.read_excel(ambhp_Storage,'90 C Process Temp')

ambhp_Storage_land = ambhp_Storage_land.drop(ambhp_Storage_land.index[0:13]).drop(ambhp_Storage_land.index[20:])

ambhp_Storage_land = ambhp_Storage_land.T.rename(columns={16:'land_use_m2_win',
                                                          19:'land_use_m2_sum',
                                                          13:'COUNTY_FIPS'}).drop(ambhp_Storage_land.T.index[0:2])
                                                            # 14, 17 high
                                                            # 15, 18 low
                                                            # 16, 19 mean

ambhp_s_land = pd.merge(ambhp_Storage_land,swh[['COUNTY_FIPS','avail_land']],on='COUNTY_FIPS')

ambhp_s_land = ambhp_s_land = ambhp_s_land[ambhp_s_land.avail_land!=0]

ambhp_s_land.loc[:,'land_perc_of_total'] = ambhp_s_land['land_use_m2_win']/(1000**2)/ambhp_s_land['avail_land']*100

ambhp_s_land.loc[:,'land_use'] = ambhp_s_land['land_use_m2_win']/(1000**2)





