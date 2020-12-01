# -*- coding: utf-8 -*-
"""
Created on Sat Oct 24 16:41:42 2020

@author: wxi
"""
import webbrowser
import time
import time
from datetime import datetime
import pandas as pd
import numpy as np
import json

def download_rate(rateid):
    '''
    script to download openei rate structure json files for processing
    
    '''
    url = "http://en.openei.org/services/rest/utility_rates?version=latest&format=json&detail=full&getpage="
    url += rateid
    webbrowser.open(url)

def create_json_rates(jsonfile):
  
    with open('./calculation_data/openei/' + jsonfile) as f:
        rates = json.load(f)

    d = datetime.today()
    year = int(d.strftime("%Y")) - 1
    date1 = str(year) + '-01-01'
    date2 = str(year+1) + '-01-01'
    
    hourlystamp = pd.date_range(date1, date2, freq = "1H").tolist()[:8760]   

    def get_energyrates(hour,day,month):
        '''
        hour: 0-23, 00:00 -> 23:00
        dayofweek 0-6, monday:sunday
        month 1-2, jan:dec
        
        '''
        
        if day not in [5,6]:
            return energyrates[energyweekday[month-1][hour]][0]['rate']
        else:
            return energyrates[energyweekend[month-1][hour]][0]['rate']

    def get_demandrates(hour,day,month):
        '''
        hour: 0-23, 00:00 -> 23:00
        dayofweek 0-6, monday:sunday
        month 1-2, jan:dec
        
        '''
       
        if day not in [5,6]:
            return demandrates[demandweekday[month-1][hour]][0]['rate']
        else:
            return demandrates[demandweekend[month-1][hour]][0]['rate'] 
 
    fixed = rates['items'][0]['fixedchargefirstmeter']       
    energyweekday =  rates['items'][0]['energyweekdayschedule']
    energyweekend = rates['items'][0]['energyweekendschedule']
    energyrates = rates['items'][0]['energyratestructure']
       
    erate_8760 = [get_energyrates(i.hour,i.dayofweek,i.month) for i in hourlystamp]   
    
    if 'flatdemandstructure' in rates['items'][0].keys():
        flatdemand = rates['items'][0]['flatdemandstructure'][0][0]['rate']
        drate_8760 = False
    else: 
        flatdemand = False
        
        demandweekday = rates['items'][0]['demandweekdayschedule']
        demandweekend = rates['items'][0]['demandweekendschedule']
        demandrates = rates['items'][0]['demandratestructure']
    
        drate_8760 = [get_demandrates(i.hour,i.dayofweek,i.month) for i in hourlystamp]
    

    
    ratesdict = {"touenergy": erate_8760, "toudemand": drate_8760, "fixed" : fixed,
                 "flatdemand": flatdemand}
    
    return ratesdict

def main():

    filepaths = ["losangeles.json", "jamescity.json", "franklin.json", "jefferson.json", "duval.json", "dickson.json",
                 "manitowoc.json", "bartholomew.json", "logan.json", "lesueur.json", "lincoln.json",
                 "garfield.json", "madison.json", "grayson.json"]
    counties = ["6037", "51095", "39049", "8059", "12031", "47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181"]
    allrates = {}
    
    for i,j in zip(filepaths,counties):
        allrates[j] = create_json_rates(i)
        
    with open('./calculation_data/allelecrates.json', 'w') as fp:
        json.dump(allrates, fp)
