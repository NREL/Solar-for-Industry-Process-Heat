# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 12:16:59 2020

@author: wxi
"""
import os.path
import os
import pandas as pd
import numpy as np
import get_8760_load as gl
import pr_par as pp
from lcoh_config import ParamMethods as pm
from model_config import ModelParams as mp
#Check if input files exist, if not run scripts to generate them if possible

path = "./calculation_data/"

#CORP TAX
assert os.path.isfile(path + "corp_tax.csv"), "Get the corporate tax csv file"
#FIPS CODES
assert os.path.isfile(path + "US_FIPS_Codes.csv"), "Get the FIPS code file"
#LAND PRICES
if not os.path.isfile(path + "landprices.csv"):
    assert os.path.isfile(path + "county_price.csv"), "Get the county price file"
    import get_land_prices as glp
    glp.main()
    
assert os.path.isfile(path + "aglandprices.csv"), "Get the AG land price file"
#ELECTRICITY PRICES
if not os.path.isfile(path + "allelecrates.json"):
    assert os.listdir(path + "openei"), "Download appropriate rate json files"
    import elec_rate_structures as ers
    #Make sure the main function has the right paths and county fips codes defined
    ers.main()

if not os.path.isfile(path + "elec_curve.json"):
    assert os.listdir(path + "eleclmp"), "Look for lmp files directory"
    import elec_pdc_curves as epc
    epc.main()
    
#BURDEN RATES
assert os.path.isfile(path + "burdenrates.csv"), "Get the burden rate file" 
#JSON files
files = ["dsg_lf_gen.json", "ptc_notes_gen.json", "ptc_tes_gen.json", "pv_ac_gen.json", "pv_dc_gen.json"]
exists = [os.path.isfile(path + f) for f in files]
if False in exists:
    assert os.path.isfile(path + "county_center.csv"), "Get the county center file"
    import h5_to_json as h5j
    h5j.main()
#furnacepload
assert os.path.isfile(path + "furnacepload.csv"), "Furnace Part Load" 
#cost index
if not os.path.isfile(path + "cost_index_data.csv"):
    assert os.path.isfile(path + "EERC_Fuel_Esc.csv"), "Get the EERC esc csv file"
    assert os.path.isfile(path + "cost_index.txt"), "Get the cost index txt file"
    import get_API_params as gap
    gap.UpdateParams.create_index()
    
#countyrural 10%
assert os.path.isfile(path + "county_rural_ten_percent_results_20200330.csv"), "Get land availability csv file"  

#load shape files
assert os.path.isfile(path + "all_load_shapes_process_heat_20200728.gzip"), "Get process heat load shape file"  
assert os.path.isfile(path + "all_load_shapes_boiler_20200728.gzip"), "Get boiler heat load shape file"  
assert os.path.isfile(path + "elec_loads.csv"), "get elec load shape csv file"  
#annual load files
assert os.path.isfile(path + "mfg_eu_temps_20200728_0810.parquet.gzip"), "get annual heat load file"  
assert os.path.isfile(path + "net_elec.csv"), "get annual elec load file"  

efficiency = False
deprc = True
# options below
casestudy = "brewery"

prh = "./calculation_data/all_load_shapes_process_heat_20200728.gzip"
boil = "./calculation_data/all_load_shapes_boiler_20200728.gzip"

if casestudy == "brewery":
    mp.deprc = deprc
    mp.boilereff = efficiency
    pm.config["measurements"]["state"] = efficiency
    pm.config["empsize"] = "n1000"
    pm.config["comb"] = "BOILER"
    pm.config["td"] = 0.25
    loadobj = gl.get_load_8760(boil, 312120, 'n1000','Conventional Boiler Use', 'Natural_gas', '6037')
    loadobj.get_shape()
    # annual heat load and elec load
    loadobj.get_annual_loads(11800555.56, 329478.282149*293.07)
    loadobj.get_8760_loads()
    #counties = ["39049", "6037", "12031", "8059", "51095"]
    #techs = ["PTCTES","PTC","PVEB", "DSGLF"]
    counties = ["39049"]
    techs = ["DSGLF"]
    
if casestudy == "aluminum":
    mp.deprc = deprc
    mp.furnaceeff = efficiency
    pm.config["measurements"]["state"] = efficiency
    pm.config["empsize"] = "n250_499"
    pm.config["comb"] = "FURNACE"
    pm.config["td"] = 0.2
    loadojb = gl.get_load_8760(prh, 331524, 'n250_499','Process Heating', 'Natural_gas', '6037')
    loadobj.get_shape()
    # annual heat load and elec load
    loadobj.get_annual_loads(18281609.82, 139183.503788*293.07)
    loadobj.get_8760_loads()
    #counties = ["47043", "55071", "18005", "21141", "27079", "29113", "40047", "47113", "48181"]
    techs = ["PVRH"]
    counties = ["47043"]

if casestudy == "brewery":
    assert pm.config["mode"] == "su", "set solar sizing mode to su"  
    #meta run generates csv files for general lcoh/pb/irr esults
    meta = True
    if meta: 
        for county in counties:
            data = pd.DataFrame()
            for tech in techs:
                 print(tech + county)
                 mult_l = np.linspace(0.1,0.2,2)
                 lcoh = []
                 year0 = []
                 fuelpar = []
                 investpar = []
                 sf = []
                 su = []
                 pb = []
                 irr = []
                 df = pd.DataFrame(index = list(np.linspace(1,2,2)))
                 df["mult"] = mult_l
                 for mult in mult_l:
                     solarform = [('REPLACE', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     lcoh.append(parity.solar_current[0][0])
                     year0.append(parity.solar[0].year0[0])
                     fuelpar.append(parity.pp_1D("FUELPRICE"))
                     investpar.append(parity.pp_1D("INVESTMENT"))
                     sf.append(parity.solar[0].sf)
                     su.append(parity.solar[0].smodel.su)
                     parity.pb_irr()
                     pb.append(parity.payback)
                     irr.append(parity.irr)
                 df["LCOH (USD Cents/kwh)"] = lcoh
                 df["Year0 (USD)"] = year0
                 df["Fuel Parity ($/1000 cuf)"] = fuelpar
                 df["USD/kwp"] = investpar
                 df["sf"] = sf
                 df["su"] = su
                 df["pb"] = pb
                 df["irr"] = irr
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + ".csv")
                 print("done")
    # specific results for payback period vs fuel price heat map 
    pbfp = False      
    if pbfp:
        for county in counties:
            for tech in techs:
                 print(county+tech)
                 df = pd.DataFrame(index = list(np.linspace(1,20,20)))
                 mult_l = np.linspace(0.1,2,20)
                 for mult in mult_l:
                     solarform = [('REPLACE', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     df[str(mult)] = parity.fp_pb(0)
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + "_pbfp.csv")
                 print("done")
                 
    # specific results for payback period vs fuel price heat map at a certain investment reduction
    pbfpi = False
    if pbfpi:
        i_reduc = 0.25
        mp.i_reduc = i_reduc
        for county in counties:
            for tech in techs:
                 print(county+tech)
                 df = pd.DataFrame(index = list(np.linspace(1,20,20)))
                 mult_l = np.linspace(0.1,2,20)
                 for mult in mult_l:
                     solarform = [('REPLACE', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     df[str(mult)] = parity.fp_pb(0)
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + "_pbfpi_" + str(i_reduc*100) + ".csv")
                 print("done")
                 
if casestudy == "aluminum":
    assert pm.config["mode"] == "su", "set solar sizing mode to su"  
    #meta run generates csv files for general lcoh/pb/irr esults
    meta = False
    if meta: 
        for county in counties:
            data = pd.DataFrame()
            for tech in techs:
                 print(tech + county)
                 mult_l = np.linspace(0.1,2,20)
                 lcoh = []
                 year0 = []
                 fuelpar = []
                 investpar = []
                 sf = []
                 su = []
                 pb = []
                 irr = []
                 df = pd.DataFrame(index = list(np.linspace(1,20,20)))
                 df["mult"] = mult_l
                 for mult in mult_l:
                     solarform = [('GREENFIELD', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     lcoh.append(parity.solar_current[0][0])
                     year0.append(parity.solar[0].year0[0])
                     fuelpar.append(parity.pp_1D("FUELPRICE"))
                     investpar.append(parity.pp_1D("INVESTMENT"))
                     sf.append(parity.solar[0].sf)
                     su.append(parity.solar[0].smodel.su)
                     parity.pb_irr()
                     pb.append(parity.payback)
                     irr.append(parity.irr)
                 df["LCOH (USD Cents/kwh)"] = lcoh
                 df["Year0 (USD)"] = year0
                 df["Fuel Parity ($/1000 cuf)"] = fuelpar
                 df["USD/kwp"] = investpar
                 df["sf"] = sf
                 df["su"] = su
                 df["pb"] = pb
                 df["irr"] = irr
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + ".csv")
                 print("done")
    # specific results for payback period vs fuel price heat map 
    pbfp = False      
    if pbfp:
        for county in counties:
            for tech in techs:
                 print(county+tech)
                 df = pd.DataFrame(index = list(np.linspace(1,20,20)))
                 mult_l = np.linspace(0.1,2,20)
                 for mult in mult_l:
                     solarform = [('GREENFIELD', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     df[str(mult)] = parity.fp_pb(0)
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + "_pbfp.csv")
                 print("done")
                 
    # specific results for payback period vs fuel price heat map at a certain investment reduction
    pbfpi = False
    if pbfpi:
        i_reduc = 0.25
        mp.i_reduc = i_reduc
        for county in counties:
            for tech in techs:
                 print(county+tech)
                 df = pd.DataFrame(index = list(np.linspace(1,20,20)))
                 mult_l = np.linspace(0.1,2,20)
                 for mult in mult_l:
                     solarform = [('GREENFIELD', tech, mult, county)]
                     combform = [("GREENFIELD", pm.config["comb"], -1, county)]
                     parity = pp.PrPar(mp, pm, form = [solarform,combform])
                     df[str(mult)] = parity.fp_pb(0)
                 df.to_csv("./calculation_data/metaresults/meta_" + county + "_" + tech + "_pbfpi_" + str(i_reduc*100) + ".csv")
                 print("done")  
