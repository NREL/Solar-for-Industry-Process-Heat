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
from matplotlib.lines import Line2D
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns    
import matplotlib.colors as c


def plotmultRH(tech, plot = "sfsu"):
    '''
    FOR PVRH Plotting
    '''
    def remove_brackets(a):
        return float(a.strip("[]"))
    
    fig, ax = plt.subplots(nrows = 3, ncols = 3, figsize = (10,10), dpi = 200);

    fuelprices = [3.438490150158424, 3.7176837948294073, 4.231987877118061, 2.9829636772741885, 
                  3.2621573219451725, 4.555264728842357, 1.8368002938880472, 3.438490150158424, 2.1159939385590305]
    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
        "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas"]  
    lcohs = [13.38163327, 13.44220613, 13.64307386, 13.16439952, 13.28778462, 13.78175301, 12.66747593, 13.38163327, 12.7952988]
    colors = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:grey", "tab:olive", "tab:cyan"]

    
    lcoh_dict = {k:v for k,v in zip(counties, lcohs)}
    county_dict = {k:v for k,v in zip(counties, names)}
    fp_dict = {k:v for k,v in zip(counties, fuelprices)}
    color_dict = {k:v for k,v in zip(counties, colors)}
    plots = [(0,0), (0,1), (0,2), (1,0), (1,1), (1,2), (2,0), (2,1), (2,2)]

    for i,j in zip(counties, list(range(9))):
        path = "./calculation_data/metaresults/no measurements/" + "meta_" + i + "_" + tech + "_.csv"
        df = pd.read_csv(path)

        df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
        df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)
        
        mult = df["mult"]
        sf = df["sf"]
        invest = df["USD/kwp"]
        invest = (invest * mult *1200/1.2 - 6383037.626107867*1.152)/(mult*1200)
        lcoh = df["LCOH (USD Cents/kwh)"]
        fp = df["Fuel Parity ($/1000 cuf)"]
        su = df["su"]
        
        x,y = plots[j]
        
        if plot == "sfsu":

            ax[x,y].plot(mult, sf, color = "tab:blue", linestyle = "solid")
            ax[x,y].plot(mult, su, color = "tab:blue", linestyle = "dashed")
            ylabel1 = 'Solar Fraction or Solar Utilization'

        if plot == "lcoh":  
            ax[x,y].plot(mult, lcoh, color = "tab:blue")
            ax[x,y].axhline(y = lcoh_dict[i], color = "tab:blue", alpha = 0.3)
            ylabel1 = 'LCOH (cents/kwh)'

        if plot == "iter":
            ax[x,y].plot(mult, invest, color = "tab:green")
            ax[x,y].axhline(y = 1580, color = "tab:green", alpha = 0.3)
            ax[x,y].set_ylim(bottom=0)
            ax2 = ax[x,y].twinx()
            ax2.plot(mult, fp, color = "tab:blue")
            ax2.axhline(y = fp_dict[i], color = "tab:blue", alpha = 0.3)
            ylabel1 = 'Parity Investment Price ($/kw install)'
            ylabel2 = 'Parity Fuel Price ($/1000 cuf)'
            
        if plot == "carbon":
            ylabel1 = 'Cost of Carbon ($/ton)'
            carbon = (df["Fuel Parity ($/1000 cuf)"] - fp_dict[i])/0.0585
            ax[x,y].plot(mult, carbon, color = "tab:blue")
            if min(carbon) > 50:
                ax[x,y].set_ylim(0,50)
            else:
                ax[x,y].set_ylim(top = 50)
            
        plt.rcParams.update({'font.size': 12})  
        ax[x,y].set_title(county_dict[i], fontsize = 12) 
        
    if plot == "sfsu": 
        custom_lines2 = [Line2D([0],[0],linestyle = i, color = "tab:blue") for i in ["solid", "dashed"]]
        ax[2,2].legend(custom_lines2, ["Solar Fraction", "Solar Utilization"], bbox_to_anchor=(1.06, -0.12), loc = "lower right", prop = {"size": 8})
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.3)    
                         
    if plot == "lcoh":
        custom_lines2 = [Line2D([0],[0],linestyle = i, color = "tab:blue") for i in ["solid"]] 
        ax[2,2].legend(custom_lines2,["LCOH (c/kwh)"], bbox_to_anchor=(1.0, 0.1), loc = "lower right", prop = {"size": 8}) 
        fig.text(0.03, 0.5, ylabel1, va='center', rotation = "vertical")      
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.4, hspace = 0.25)     
        
    if plot == "iter":
        custom_lines2 = [Line2D([0],[0],linestyle = "solid", color = "tab:green"), Line2D([0],[0],linestyle = "solid", color = "tab:blue")]
        ax[2,2].legend(custom_lines2,["Parity Investment Price", "Parity Fuel Price"], bbox_to_anchor=(1.02, 0.1), loc = "lower right", prop = {"size": 8}) 
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1.02, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.3)   
        
    if plot == "carbon":
        custom_lines2 = [Line2D([0],[0],linestyle = i, color = "tab:blue") for i in ["solid"]] 
        ax[2,2].legend(custom_lines2,["Carbon Cost ($/ton)"], bbox_to_anchor=(1.0, 0.1), loc = "lower right", prop = {"size": 8}) 
        fig.text(0.03, 0.5, ylabel1, va='center', rotation = "vertical")      
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.4, hspace = 0.25)         

    fig.text(0.5, 0.08,'PV Field Size (MWAC)', ha='center') 

def plotcountyrh(tech, plot = "lcoh"):
    
    def remove_brackets(a):
        return float(a.strip("[]"))
    
    fig, ax = plt.subplots(figsize = (10,10), dpi = 200);

    fuelprices = [3.438490150158424, 3.7176837948294073, 4.231987877118061, 2.9829636772741885, 
                  3.2621573219451725, 4.555264728842357, 1.8368002938880472, 3.438490150158424, 2.1159939385590305]
    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181"]
    lcohs = [13.38163327, 13.44220613, 13.64307386, 13.16439952, 13.28778462, 13.78175301, 12.66747593, 13.38163327, 12.7952988 ]
    colors = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:grey", "tab:olive", "tab:cyan"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
            "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas"]
    
    lcoh_dict = {k:v for k,v in zip(counties, lcohs)}
    county_dict = {k:v for k,v in zip(counties, names)}
    fp_dict = {k:v for k,v in zip(counties, fuelprices)}
    color_dict = {k:v for k,v in zip(counties, colors)}    
    
    for i in counties:
        path = "./calculation_data/metaresults/no measurements/" + "meta_" + i + "_" + tech + "_.csv"
        df = pd.read_csv(path)

        df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
        df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)
        
        mult = df["mult"]
        sf = df["sf"]
        invest = df["USD/kwp"]
        invest = (invest * mult *1200/1.2 - 6383037.626107867*1.152)/(mult*1200)
        lcoh = df["LCOH (USD Cents/kwh)"]
        fp = df["Fuel Parity ($/1000 cuf)"]
        su = df["su"]
        
        if plot == "lcoh":  
            ax.plot(mult, lcoh, color = color_dict[i])
            ylabel1 = 'LCOH (cents/kwh)'
        if plot == "sf":
            ax.plot(mult, sf, color = color_dict[i])
            ylabel1 = 'Solar Fraction'
        if plot == "su":
            ax.plot(mult, su, color = color_dict[i])
            ylabel1 = 'Solar Utilization'
            
        plt.rcParams.update({'font.size': 12})  
    
    #ax.axhline(y = 13.8, color = "black", alpha = 0.3)
    custom_lines2 = [Line2D([0],[0],linestyle = "solid", color = color_dict[i]) for i in counties ]
    ax.legend(custom_lines2, [county_dict[i] for i in counties], bbox_to_anchor=(1.0, 0.1), loc = "lower right", prop = {"size": 14}) 
    fig.text(0.03, 0.5, ylabel1, va='center', rotation = "vertical", fontsize = 14)      
    fig.text(0.5, 0.05,'PV Field Size (MWAC)', ha='center', fontsize = 14) 
    plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.4, hspace = 0.25)     
        
   
    
def ploteff(county, techlist = ["DSGLF", "PTCTES", "PTC", "PVEB"] ):
    
    def remove_brackets(a):
        return float(a.strip("[]"))
    
    fig, ax = plt.subplots(dpi = 100);
    ax2 = ax.twinx();
    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181", "39049", "6037", "12031", "8059", "51095"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
             "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas", "Franklin County, Ohio", "Los Angeles, California", 
             "Duval County, Florida", "Jefferson County, Colorado", "James City, Virginia"]  
    
    linestyle_dict = {"DSGLF": 'solid', 'PTCTES': 'dashed', 'PTC': 'dashdot', 'PVEB': 'dotted', 'PVRH': "solid"}
    county_dict = {k:v for k,v in zip(counties, names)}
    for i in techlist:
        path = "./calculation_data/metaresults/measurements/" + "meta_" + county + "_" + i + "_.csv"
        path1 = "./calculation_data/metaresults/no measurements/" + "meta_" + county + "_" + i + "_.csv"
        
        df = pd.read_csv(path)
        df1 = pd.read_csv(path1)

        df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
        df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)
        df1["Fuel Parity ($/1000 cuf)"] =  df1["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
        df1["USD/kwp"] = df1["USD/kwp"].apply(remove_brackets)
        
        sfeff = df["sf"]
        fpeff = df["Fuel Parity ($/1000 cuf)"]
        multeff = df["mult"]

        sfnoeff = df1["sf"]
        fpnoeff = df1["Fuel Parity ($/1000 cuf)"]
        multnoeff = df1["mult"]
        
        color = 'tab:orange'
        color2 = 'tab:red'
        color3 = 'tab:green'
        color4 = 'tab:purple'
        
        ax.set_xlabel('Solar Fraction');
        ax.set_ylabel('Fuel Price ($/1000 cuf)');
        ax.plot(sfeff, fpeff, color = color3, linestyle = linestyle_dict[i])#, marker = marker_dict[i], linestyle='None');
        ax.plot(sfnoeff, fpnoeff, color = color4, linestyle = linestyle_dict[i])

        
    custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
    custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = 'tab:green'),Line2D([0],[0],linestyle = 'solid', color = 'tab:purple')]
    ax2.legend(custom_lines, techlist, loc = 0)
    ax2.set_yticks([])
    ax2.set_yticklabels([])
    ax.legend(custom_lines2,["With Eff", "Without Eff"], loc = 2)
    ax.set_title(" ".join([county, county_dict[county]]))
        
    fig.tight_layout()

def plotophour(county, tech, mode = "fp"):
    
    def remove_brackets(a):
        try:
            return float(a.strip("[]"))
        except ValueError:
            return np.nan
    
    fig, ax = plt.subplots(dpi = 100);
    
    if tech not in ["PVRH"]:
        peaklow = 2740
        peakhigh = 1851
        peakavg = 2164

    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181", "39049", "6037", "12031", "8059", "51095"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
             "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas", "Franklin County, Ohio", "Los Angeles, California", 
             "Duval County, Florida", "Jefferson County, Colorado", "James City, Virginia"]  
    county_dict = {k:v for k,v in zip(counties,names)}

    path = "./calculation_data/metaresults/no measurements/" + "meta_" + county + "_" + tech + "_.csv"
    path1 = "./calculation_data/metaresults/no measurements high/" + "meta_" + county + "_" + tech + "_.csv"
    path2 = "./calculation_data/metaresults/no measurements low/" + "meta_" + county + "_" + tech + "_.csv"

    df = pd.read_csv(path)
    df1 = pd.read_csv(path1)
    df2 = pd.read_csv(path2)

    df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
    df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)
    df1["Fuel Parity ($/1000 cuf)"] =  df1["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
    df1["USD/kwp"] = df1["USD/kwp"].apply(remove_brackets)
    df2["Fuel Parity ($/1000 cuf)"] =  df2["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
    df2["USD/kwp"] = df2["USD/kwp"].apply(remove_brackets)

    multlow = df2["mult"]
    fplow = df2["Fuel Parity ($/1000 cuf)"]
    fplowmask = np.isfinite(fplow)
    lcohlow = df2["LCOH (USD Cents/kwh)"]
    sflow = df2["sf"]
    sulow = df2["su"]

    multhigh = df1["mult"]
    fphigh = df1["Fuel Parity ($/1000 cuf)"]
    fphighmask = np.isfinite(fphigh)
    lcohhigh = df1["LCOH (USD Cents/kwh)"]
    sfhigh = df1["sf"]
    suhigh = df1["su"]
    
    mult = df["mult"]
    fp = df["Fuel Parity ($/1000 cuf)"]
    fpmask = np.isfinite(fp)
    lcoh = df["LCOH (USD Cents/kwh)"]
    sf = df["sf"]
    su = df["su"]

    color = 'tab:orange'
    color2 = 'tab:red'
    color3 = 'tab:green'
    color4 = 'tab:purple'
    
    ax.set_title(" ".join([county, county_dict[county], tech]))
    ax.set_xlim(0,2)
    
    ax.set_xlabel('Ratio of Solar System Size to Peak load (MWth/MWth)');

    custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = color3),
                     Line2D([0],[0],linestyle = 'solid', color = color4),
                     Line2D([0],[0],linestyle = 'dashed', color = color3),
                     Line2D([0],[0],linestyle = 'dashed', color = color4),
                     Line2D([0],[0],linestyle = 'dotted', color = color3),
                     Line2D([0],[0],linestyle = 'dotted', color = color4)]
    
    legendlabels =  ["Parity Fuel Price Avg Op.hours", "LCOH Avg Op.hours", 
                     "Parity Fuel Price High Op.hours", "LCOH High Op.hours", 
                     "Parity Fuel Price Low Op.hours", "LCOH Low Op.hours"]
    
    if mode == "fp":
        ax.plot(multlow[fplowmask], fplow[fplowmask], color = color3, linestyle = "dotted")
        ax.plot(multhigh[fphighmask], fphigh[fphighmask], color = color3, linestyle = "dashed")
        ax.plot(mult[fpmask], fp[fpmask], color = color3, linestyle = "solid")
        ax.set_ylabel('Fuel Price ($/1000 cuf)');
        custom_lines2 = custom_lines2[0::2]
        legendlabels = legendlabels[0::2]
        ax.legend(custom_lines2,legendlabels)

    if mode == "lcoh":
        ax.plot(multlow[fplowmask], lcohlow[fplowmask], color = color4, linestyle = "dotted")
        ax.plot(multhigh[fphighmask], lcohhigh[fphighmask], color = color4, linestyle = "dashed")
        ax.plot(mult[fpmask], lcoh[fpmask], color = color4, linestyle = "solid")
        ax.set_ylabel("LCOH (cents/kwh)")   
        custom_lines2 = custom_lines2[1::2]
        legendlabels = legendlabels[1::2]
        ax.legend(custom_lines2,legendlabels)
        
    if mode == "sfsu":
        ax.plot(multlow, sflow, color = color, linestyle = "dotted")
        ax.plot(multhigh, sfhigh, color = color, linestyle = "dashed")
        ax.plot(mult, sf, color = color, linestyle = "solid")
        ax.plot(multlow, sulow, color = color2, linestyle = "dotted")
        ax.plot(multhigh, suhigh, color = color2, linestyle = "dashed")
        ax.plot(mult, su, color = color2, linestyle = "solid")
        ax.set_ylabel("SF or SU") 
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = color),
                         Line2D([0],[0],linestyle = 'solid', color = color2),
                         Line2D([0],[0],linestyle = 'dashed', color = color),
                         Line2D([0],[0],linestyle = 'dashed', color = color2),
                         Line2D([0],[0],linestyle = 'dotted', color = color),
                         Line2D([0],[0],linestyle = 'dotted', color = color2)]
        legendlabels = ["SF Avg Op.hours", "SU Avg Op.hours", "SF High Op.hours", "SU High Op.hours", "SF Low Op.hours", "SU Low Op.hours"]
        ax.legend(custom_lines2,legendlabels, prop={'size': 6})    
        
    if mode == "sfsun":
        ax.plot(multlow/peaklow*1000, sflow, color = color, linestyle = "dotted")
        ax.plot(multhigh/peakhigh*1000, sfhigh, color = color, linestyle = "dashed")
        ax.plot(mult/peakavg*1000, sf, color = color, linestyle = "solid")
        ax.plot(multlow/peaklow*1000, sulow, color = color2, linestyle = "dotted")
        ax.plot(multhigh/peakhigh*1000, suhigh, color = color2, linestyle = "dashed")
        ax.plot(mult/peakavg*1000, su, color = color2, linestyle = "solid")
        ax.set_ylabel("SF or SU") 
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = color),
                         Line2D([0],[0],linestyle = 'solid', color = color2),
                         Line2D([0],[0],linestyle = 'dashed', color = color),
                         Line2D([0],[0],linestyle = 'dashed', color = color2),
                         Line2D([0],[0],linestyle = 'dotted', color = color),
                         Line2D([0],[0],linestyle = 'dotted', color = color2)]
        legendlabels = ["SF Avg Op.hours", "SU Avg Op.hours", "SF High Op.hours", "SU High Op.hours", "SF Low Op.hours", "SU Low Op.hours"]
        ax.legend(custom_lines2,legendlabels, prop={'size': 6})  
    
def heatmap(solarmodel, mode = "gensf"):
   
    model = solarmodel
    
    if mode == "gensf":
        data = np.divide(model.smodel.gen, model.load_8760)
        data[np.isinf(data)] = np.nan
        title = " ".join([model.tech_type, model.county, "Uncapped Solar Fraction Heat Map"])
    if mode == "gen":
        data =  np.array(model.smodel.gen)
        title = " ".join([model.tech_type, model.county, "Solar Generation"])
    if mode == "sf":
        data = np.divide(model.smodel.load_met, model.load_8760)
        data[np.isinf(data)] = np.nan
        title = " ".join([model.tech_type, model.county, "Solar Fraction Heat Map"])
    if mode == "su":
        data = np.divide(model.smodel.load_met, model.smodel.gen)
        data[model.smodel.gen == 0] = np.nan        
        title = " ".join([model.tech_type, model.county, "Solar Utilization Heat Map - 6 hrs Storage"])
    if mode == "ldn":
        data = (model.load_8760 - np.array(model.smodel.load_met))/model.peak_load
        data[model.load_8760 == 0] = np.nan
        title = " ".join([model.tech_type, model.county, "Normalized Supply Demand Gap Heat Map - 6 hrs Storage"])
    if mode == "load8760":
        data = model.load_8760/model.peak_load
        title = " ".join(["Hourly Load Profile Heat Map"])        
    if mode == "bf":
        data = np.divide(np.array(model.dmodel.load_met),model.load_8760)
        data[np.isinf(data)] = np.nan
        title = " ".join([model.tech_type, model.county, "Boiler Fraction Heat Map"])
        
    sfdata = pd.DataFrame(
            {"SF": data},
            index = pd.date_range('2019-01-01', '2020-01-01', freq='1H', closed='left')  
         )
    sfdata = sfdata.groupby([sfdata.index.month, sfdata.index.hour]).mean().unstack()
    sfdata.columns = sfdata.columns.droplevel(0)
    sfdata = sfdata.transpose()
    sfdata.index = list(range(1,25))       
    sfdata = sfdata.iloc[::-1]

    fig,ax = plt.subplots(figsize=(12,8))
    r = sns.heatmap(sfdata, ax = ax, cmap='YlOrRd', annot = True, cbar = False)
    ax.set_ylabel("Hour of Day")
    ax.set_xlabel("Month")
    r.set_facecolor("#ffffcc")
    r.set_title(title)
    
def plotmult(county, measure = False, plot = "iter", techlist = ["DSGLF", "PTCTES", "PTC", "PVEB"] ):
    '''
    data vs mult 
    '''
    def remove_brackets(a):
        return float(a.strip("[]"))
    
    fig, ax = plt.subplots(nrows = 2, ncols = 3, dpi = 200);
    lcoh_dict = {"39049": 3.48, "6037": 4.08, "12031" : 3.51, "8059": 3.16, "51095": 2.92}
    county_dict = {"39049": "Franklin County, Ohio", "6037": "Los Angeles, California", 
                   "12031" : "Duval County, Florida", "8059": "Jefferson County, Colorado", "51095": "James City, Virginia"}
    fp_dict = {"39049": 4.73, "6037": 5.86, "12031" : 4.69, "8059": 4.08, "51095": 3.55}
    linestyle_dict = {"DSGLF": 'solid', 'PTCTES': 'dashed', 'PTC': 'dashdot', 'PVEB': 'dotted'}
    plotloc = {0: (0,0), 1: (0,1), 2: (0,2), 3:(1,0), 4:(1,1)}

    k = 0
    for j in county:
        x, y = plotloc[k]
        
        if plot in ["carbon", "invest"]:
            pass
        else:
            ax2 = ax[x,y].twinx();
        
        for i in techlist:

            if measure:
                path = "./calculation_data/metaresults/measurements/" + "_".join(["meta", j, i, ".csv"]) 
            else:
                path = "./calculation_data/metaresults/no measurements/" + "_".join(["meta", j, i, ".csv"]) 

            df = pd.read_csv(path)

            df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
            df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)

            sf = df["sf"]
            su = df["su"]
            lcoh = df["LCOH (USD Cents/kwh)"]
            mult = df["mult"]
            fp = df["Fuel Parity ($/1000 cuf)"]
            kwtechm2 = {"DSGLF": 1200/3081.6, "PTC": 1500/2624, "PTCTES": 2500/5248}
            
            if i in ["DSGLF", "PTC", "PTCTES"]:
                invest = df["USD/kwp"] * kwtechm2[i]
            else:
                invest = df["USD/kwp"]

            if i == "PVEB":
                invest = invest/1.2
            else:
                invest = invest/1.285

            color = 'tab:orange'
            color2 = 'tab:red'
            color3 = 'tab:green'
            color4 = 'tab:blue'
            color5 = 'tab:purple'

            if plot == "solar":
                
                ax[x,y].plot(mult, sf, color = color, linestyle = linestyle_dict[i])
                ax[x,y].plot(mult, su, color = color2, linestyle = linestyle_dict[i])
                ylabel1 = 'Solar Fraction or Solar Utilization'
                ylabel2 = 'LCOH (cents/kwh)' 
                ax2.plot(mult, lcoh, color = color3, linestyle = linestyle_dict[i])
                ax2.axhline(y = lcoh_dict[j], color = color5 , alpha = 0.3)

            if plot == "iter":  
                ax[x,y].plot(mult, invest, color = color3, linestyle = linestyle_dict[i])
                ax[x,y].axhline(y = 235, color = "tab:green", alpha = 0.3)
                ax[x,y].set_ylim(bottom = 0)
                ylabel1 = 'Parity Investment Price ($/m2 aperture)'
                ylabel2 = 'Parity Fuel Price ($/1000 cuf)'
                ax2.plot(mult, fp, color = color4, linestyle = linestyle_dict[i])
                ax2.axhline(y = fp_dict[j], color = "tab:blue", alpha = 0.3)
                
            if plot == "sf":
                ax[x,y].plot(sf, invest, color = color3, linestyle = linestyle_dict[i])
                ax[x,y].axhline(y = 235, color = color5, alpha = 0.3)
                ylabel1 = 'Parity Investment Price ($/m2 aperture)'
                ylabel2 = 'Parity Fuel Price ($/1000 cuf)'
                ax2.plot(sf, fp, color = color4, linestyle = linestyle_dict[i])
                ax2.axhline(y = fp_dict[j], color = color2, alpha = 0.3)         
            if plot == "carbon":
                ylabel1 = 'Price of Carbon ($/ton)'
                carbon = (df["Fuel Parity ($/1000 cuf)"] - fp_dict[j])/0.0585
                ax[x,y].plot(mult, carbon, color = color4, linestyle = linestyle_dict[i])
                if j != "6037":
                    ax[x,y].set_xlim(0,1)
                ax[x,y].set_ylim(top = 50)            
            if plot == "invest":
                ylabel1 = 'Parity Investment Price ($/m2 aperture)'
                ax[x,y].plot(mult, invest, color = color3, linestyle = linestyle_dict[i])
                ax[x,y].set_ylim(bottom = 150)  
                if j != "6037":
                    ax[x,y].set_xlim(0,1)
            plt.rcParams.update({'font.size': 8})  
            ax[x,y].set_title(county_dict[j], fontsize = 8)

        k += 1 
        
    ax2 = ax[1,2].twinx()
    ax2.axis("off")
    ax[1,2].axis("off")    
    
    if plot == "solar": 
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = i) for i in [color,color2,color3,color5]]
        ax[1,2].legend(custom_lines2, ["Solar Fraction", "Solar Utilization", "LCOH", "Current LCOH"], bbox_to_anchor=(1.06, -0.12), loc = "lower right", prop = {"size": 8})
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
        ax2.legend(custom_lines, techlist, bbox_to_anchor=(0.15, 1.08), loc = "upper left", prop = {"size": 8})  
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.3)        
    if plot == "iter":
        colors = [color3, color4, "tab:green", "tab:blue" ]
        alphas = [1,1,0.3,0.3]
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = i, alpha = j) for i,j in zip(colors, alphas)]    
        ax[1,2].legend(custom_lines2,["Parity Investment", "Parity Fuel Price", "Current Investment", "Current Fuel Price"], bbox_to_anchor=(1.13, -0.06), loc = "lower right", prop = {"size": 8}) 
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
        ax2.legend(custom_lines, techlist, bbox_to_anchor=(0.15, 1.03), loc = "upper left", prop = {"size": 8})  
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.3)        
    if plot == "sf":
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = i) for i in [color3, color4, color5, color2]]    
        ax[1,2].legend(custom_lines2,["Parity Investment", "Parity Fuel Price", "Current Investment", "Current Fuel Price"], bbox_to_anchor=(1.13, -0.06), loc = "lower right", prop = {"size": 8}) 
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
        ax2.legend(custom_lines, techlist, bbox_to_anchor=(0.15, 1.03), loc = "upper left", prop = {"size": 8}) 
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.3)          
    if plot == "carbon":
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]  
        ax[1,2].legend(custom_lines, techlist, bbox_to_anchor=(0.8, 0.25), loc = "lower right", prop = {"size": 8})    
        fig.text(0.02, 0.5, ylabel1, va='center', rotation = "vertical")     
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.5, hspace = 0.32)
    if plot == "invest":
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i], color = color3) for i in techlist]  
        ax[1,2].legend(custom_lines, techlist, bbox_to_anchor=(0.8, 0.25), loc = "lower right", prop = {"size": 8}) 
        fig.text(0.01, 0.5, ylabel1, va='center', rotation = "vertical") 
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.4, hspace = 0.3)
    if plot in ["sf"]:  
        fig.text(0.5, 0.05,'Solar Fraction', ha='center')   
    else:
        fig.text(0.5, 0.03,'Solar System Size (MWt)', ha='center') 

def plotloadshape(case = "boiler"):
    if case == "boiler":
        df = pd.read_csv("./calculation_data/loads_8760_n1000_312120.csv")
    if case == "furnace":
        df = pd.read_csv("./calculation_data/loads_8760_n250_499_331524.csv")
    fig, ax = plt.subplots(nrows = 1, ncols = 3,dpi = 200)
    ax[0].plot(list(range(24)),df["hload"][4367:4391]/max(df["hload"]), color = "blue", label = "Average")
    ax[0].legend(loc = "center right", prop = {"size": 6})
    ax[1].plot(list(range(24)),df["hload_h"][4367:4391]/max(df["hload_h"]), color = "blue", label = "High")
    ax[1].legend(loc = "center right", prop = {"size": 6})
    ax[2].plot(list(range(24)),df["hload_l"][4367:4391]/max(df["hload_l"]), color = "blue", label = "Low")
    ax[2].legend(loc = "center right", prop = {"size": 6})
    x1 = 24
    x0 = 0
    y1 = 1
    y0 = 0
    ax[0].set_aspect((x1-x0)/(y1-y0))
    ax[1].set_aspect((x1-x0)/(y1-y0))
    ax[2].set_aspect((x1-x0)/(y1-y0))
    plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.3)
    fig.text(0.5, 0.72,'Heat Load Shapes', ha='center')
    fig.text(0.5, 0.22,'Hour of Day', ha='center')
    fig.text(0.02, 0.5, "Fractional Part Load", va='center', rotation='vertical')
    
    
    
def pbirr(county, plot = "mult", techlist = ["DSGLF", "PTCTES", "PTC", "PVEB"] ):
    '''
    data vs mult 
    '''
    def convertfalse(x):
        try:
            float(x)
            return float(x)
        except:
            return np.nan
    
    fig, ax = plt.subplots(nrows = 2, ncols = 3, dpi = 200);
    #fig,ax = plt.subplots(nrows=3, ncols=3, dpi = 200)
    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181", "39049", "6037", "12031", "8059", "51095"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
             "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas", "Franklin County, Ohio", "Los Angeles, California", 
             "Duval County, Florida", "Jefferson County, Colorado", "James City, Virginia"]  
    county_dict = {k:v for k,v in zip(counties,names)}
    linestyle_dict = {"DSGLF": 'solid', 'PTCTES': 'dashed', 'PTC': 'dashdot', 'PVEB': 'dotted', "PVRH": "solid"}
    plotloc = {0: (0,0), 1: (0,1), 2: (0,2), 3:(1,0), 4:(1,1)}
    #plotloc = {0: (0,0), 1: (0,1), 2: (0,2), 3:(1,0), 4:(1,1), 5:(1,2), 6:(2,0), 7:(2,1), 8:(2,2)}

    k = 0
    for j in county:
        x, y = plotloc[k]
        ax2 = ax[x,y].twinx();
        
        for i in techlist:

            path = "./calculation_data/metaresults/no measurements/" + "_".join(["meta", j, i, "pbirr.csv"]) 

            df = pd.read_csv(path, dtype = str)
            sf = df["sf"]
            lcoh = df["LCOH (USD Cents/kwh)"]
            mult = np.linspace(0.1,2,20)
            pb = df["pb"].apply(convertfalse)
            pbmask = np.isfinite(pb)
            irr = df["irr"].apply(convertfalse)
            irrmask = np.isfinite(irr)
            
            color = 'tab:green'
            color2 = 'tab:purple'

            if plot == "sf":
                
                ax[x,y].plot(sf[irrmask], irr[irrmask], color = color, linestyle = linestyle_dict[i])
                ax2.plot(sf[pbmask], pb[pbmask], color = color2, linestyle = linestyle_dict[i])
                ax[x,y].set_xticks(np.linspace(0,1,6))
                ax[x,y].set_xticklabels([str(i) for i in np.linspace(0,1,6)])
                ax2.set_xticks(np.linspace(0,1,6))
                ax2.set_xticklabels([str(i) for i in np.linspace(0,1,6)])
                ax[x,y].set_xlim(0,0.5)
                ax2.set_xlim(0,0.5)
                ax2.set_ylim(0,100)
                ylabel1 = 'Internal Rate of Return'
                ylabel2 = 'Payback Period (Years)' 

            if plot == "mult":  
                ax[x,y].plot(mult[irrmask], irr[irrmask], color = color, linestyle = linestyle_dict[i])
                ax2.plot(mult[pbmask], pb[pbmask], color = color2, linestyle = linestyle_dict[i])
                ax[x,y].set_xticks(np.linspace(0,5,6))
                ax[x,y].set_ylim(0,0.2)
                ax2.set_xticks(np.linspace(0,5,6))
                ax[x,y].set_xlim(0,5)
                ax2.set_xlim(0,5)
                ax2.set_ylim(0,100)
                ax[x,y].axhline(y = 0.0723, color = color, alpha = 0.3)
                ax2.axhline(y = 25, color = color2, alpha = 0.3)
                ylabel1 = 'Internal Rate of Return'
                ylabel2 = 'Payback Period (Years)'
                
            ax[1,2].axis("off") 
                
            plt.rcParams.update({'font.size': 8})  
            ax[x,y].set_title(county_dict[j], fontsize = 8)
            
        k += 1 
    
    if plot == "sf": 
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = i) for i in [color,color2]]
        #ax[1,2].legend(custom_lines2, ["IRR", "Payback Period"], bbox_to_anchor=(1.06, -0.12), loc = "lower right", prop = {"size": 8})
        ax[2,2].legend(custom_lines2, ["IRR", "Payback Period"], bbox_to_anchor=(1.06, -0.12), loc = "lower right", prop = {"size": 8})
        #custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
        #ax2.legend(custom_lines, techlist, bbox_to_anchor=(0.15, 1.08), loc = "upper left", prop = {"size": 8})  
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.8, hspace = 0.4)
        
    if plot == "mult":
        custom_lines2 = [Line2D([0],[0],linestyle = 'solid', color = i) for i in [color,color2]]  
        ax[1,2].legend(custom_lines2,["IRR", "Payback Period"], bbox_to_anchor=(1.05, -0.06), loc = "lower right", prop = {"size": 8}) 
        #ax[2,2].legend(custom_lines2,["IRR", "Payback Period"], bbox_to_anchor=(1.03, 0.5), loc = "lower right", prop = {"size": 6}) 
        custom_lines = [Line2D([0],[0],linestyle = linestyle_dict[i]) for i in techlist]   
        ax2.legend(custom_lines, techlist, bbox_to_anchor=(1.7, 0.95), loc = "upper left", prop = {"size": 8})  
        fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
        fig.text(1, 0.5, ylabel2, va='center', rotation= 270)
        plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.55, hspace = 0.4)
        #fig.text(0, 0.5, ylabel1, va='center', rotation = "vertical")      
       # fig.text(1.03, 0.5, ylabel2, va='center', rotation= 270)
        #plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.65, hspace = 0.6)

    if plot in ["sf"]:  
        fig.text(0.5, 0.05,'Solar Fraction', ha='center')   
    else:
        fig.text(0.5, 0.05,'Solar System Size (MWt)', ha='center') 
        #fig.text(0.5,0.03,"PV Field Size (MWAC)", ha = "center")
        

def plotdickson():
    '''
    FOR PVRH Plotting
    '''
    def remove_brackets(a):
        return float(a.strip("[]"))
    
    fig, ax = plt.subplots(figsize = (10,10), dpi = 200);

    fuelprices = [3.438490150158424, 3.7176837948294073, 4.231987877118061, 2.9829636772741885, 
                  3.2621573219451725, 4.555264728842357, 1.8368002938880472, 3.438490150158424, 2.1159939385590305]
    counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181"]
    names = ["Dickson, Tennessee", "Manitowoc, Wisconsin", "Bartholomew, Indiana", "Logan, Kentucky", "Le Sueur, Minnesota",
        "Lincoln, Missouri", "Garfield, Oklahoma", "Madison, Tennessee", "Grayson, Texas"]  
    lcohs = [13.38163327, 13.44220613, 13.64307386, 13.16439952, 13.28778462, 13.78175301, 12.66747593, 13.38163327, 12.7952988]
    colors = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:grey", "tab:olive", "tab:cyan"]

    
    lcoh_dict = {k:v for k,v in zip(counties, lcohs)}
    county_dict = {k:v for k,v in zip(counties, names)}
    fp_dict = {k:v for k,v in zip(counties, fuelprices)}
    color_dict = {k:v for k,v in zip(counties, colors)}
    plots = [(0,0), (0,1), (0,2), (1,0), (1,1), (1,2), (2,0), (2,1), (2,2)]
    
    tech = "PVRH"
    county = "47043"
    
    path = "./calculation_data/metaresults/no measurements/" + "meta_" + county + "_" + tech + "_.csv"
    df = pd.read_csv(path)

    df["Fuel Parity ($/1000 cuf)"] =  df["Fuel Parity ($/1000 cuf)"].apply(remove_brackets)
    df["USD/kwp"] = df["USD/kwp"].apply(remove_brackets)

    mult = df["mult"]
    sf = df["sf"]
    invest = df["USD/kwp"]
    invest = (invest * mult *1200/1.2 - 6383037.626107867*1.152)/(mult*1200)
    lcoh = df["LCOH (USD Cents/kwh)"]
    fp = df["Fuel Parity ($/1000 cuf)"]
    su = df["su"]

    ax.plot(mult, lcoh, color = "tab:blue")
    for i,j in zip([13.38, 17.20, 21.05, 24.94, 28.87, 32.85, 36.86, 40.92], [1.5,2,2.5,3,3.5,4,4.5,5]):
        ax.axhline(y = i, color = "tab:blue", alpha = 0.3)
        ax.text(1, i+0.1, str(j) + "% melt loss")


    ylabel1 = 'LCOH (cents/kwh)'

    plt.rcParams.update({'font.size': 12})  
    ax.set_title(county_dict[county], fontsize = 12) 


    custom_lines2 = [Line2D([0],[0],linestyle = i, color = "tab:blue") for i in ["solid"]] 
    ax.legend(custom_lines2,["LCOH (c/kwh)"], bbox_to_anchor=(1.0, 0.1), loc = "lower right", prop = {"size": 8}) 
    fig.text(0.03, 0.5, ylabel1, va='center', rotation = "vertical")      
    plt.subplots_adjust(left = 0.1, right = 0.95, wspace = 0.4, hspace = 0.25)     
        

    fig.text(0.5, 0.08,'PV Field Size (MWAC)', ha='center') 
    

def pbfp(county, tech):
    
    def convertfalse(x):
        try:
            float(x)
            if float(x) == 0:
                return np.nan
            else:
                return float(x)
        except:
            return np.nan  
        
    path = "./calculation_data/metaresults/no measurements/" + "_".join(["meta", county, tech, "pbfp.csv"])  
    data = pd.read_csv(path)
    data.drop(columns = ["Unnamed: 0"], inplace = True)
    data.columns = [str(i) for i in np.round(np.linspace(0.1,2,20),1)]
    data.index = [str(i) for i in np.round(np.linspace(1,20,20),0)]

    data = data.applymap(convertfalse)
    data = data.iloc[::-1]
    fig,ax = plt.subplots(figsize=(12,8))
    r = sns.heatmap(data, ax = ax, mask = data >5, cmap = ["#00491d"], annot = True, cbar = False)
    r = sns.heatmap(data, ax = ax, mask = data <=5, cmap = 'BuGn_r', annot = True, cbar = True)
    ax.set_ylabel("Fuel Price ($/1000cuf)")
    ax.set_xlabel("Solar System Size (MWt)")
    ax.hlines(14, *ax.get_xlim())
    ax.legend([Line2D([0],[0],linestyle = 'solid', color = "black")], ["Current Fuel Price"], loc = "lower right")
    r.set_facecolor("#D3D3D3")
    title = " ".join([tech, county, "Payback Period (Years)"])
    r.set_title(title)

    
def pbfpi(county, tech):
    
    def convertfalse(x):
        try:
            float(x)
            if float(x) == 0:
                return np.nan
            else:
                return float(x)
        except:
            return np.nan  
        
    path = "./calculation_data/metaresults/no measurements/" + "_".join(["meta", county, tech, "pbfp.csv"])  
    path2 = "./calculation_data/metaresults/no measurements/" + "_".join(["meta", county, tech, "pbfpi.csv"])
    
    data = pd.read_csv(path)
    data2 = pd.read_csv(path2)
    

    data.drop(columns = ["Unnamed: 0"], inplace = True)
    data.columns = [str(i) for i in np.round(np.linspace(0.1,2,20),1)]
    data.index = [str(i) for i in np.round(np.linspace(1,20,20),0)]
    data = data.applymap(convertfalse)
    data = data.iloc[::-1]
        

    data2.drop(columns = ["Unnamed: 0"], inplace = True)
    data2.columns = [str(i) for i in np.round(np.linspace(0.1,2,20),1)]
    data2.index = [str(i) for i in np.round(np.linspace(1,20,20),0)]
    data2 = data2.applymap(convertfalse)
    data2 = data2.iloc[::-1]
    
    fig,ax = plt.subplots(1,2,figsize=(20,12))
    r = sns.heatmap(data, ax = ax[0], mask = data >5, cmap = ["#00491d"], annot = True, cbar = False)
    r = sns.heatmap(data, ax = ax[0], mask = data <=5, cmap = 'BuGn_r', annot = True, cbar = False)
    r2 = sns.heatmap(data2, ax = ax[1], mask = data2 >5, cmap = ["#00491d"], annot = True, cbar = False)
    r2 = sns.heatmap(data2, ax = ax[1], mask = data2 <=5, cmap = 'BuGn_r', annot = True, cbar = False)
    
    for i in ax:
        i.set_ylabel("Fuel Price ($/1000cuf)")
        i.set_xlabel("Solar System Size (MWt)")
        i.set_ylabel("Fuel Price ($/1000cuf)")
        i.set_xlabel("Solar System Size (MWt)")    
        i.hlines(16, *i.get_xlim())
        
    ax[1].legend([Line2D([0],[0],linestyle = 'solid', color = "black")], ["Current Fuel Price"], loc = "lower right")
    r.set_facecolor("#D3D3D3")
    r2.set_facecolor("#D3D3D3")  
    title = fig.suptitle(" ".join([tech, county, "Payback Period"]), fontsize = 20, fontweight = "bold")
    title.set_y(0.93)

if __name__ == "__main__":
    pass