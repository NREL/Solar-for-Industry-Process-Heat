# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 17:03:09 2020

@author: wxi
"""
import pandas as pd

landprice = pd.read_csv("./calculation_data/county_price.csv")
landprice["state_fips"] = round(landprice["County"], -3)
colnames = ["County Name", "County GEOID", "Neighbor Name", "Neighbor GEOID"]
dtypes = ["str", "float", "str", "int"]
dtypes = {k:v for k,v in zip(colnames,dtypes)}
adj_county = pd.read_csv("./calculation_data/county_adjacency.txt", sep = '\t', lineterminator = "\n", names = colnames, dtype = dtypes, encoding = "ISO-8859-1")
adj_county.fillna(method = "ffill", inplace = True)
adj_county ["County GEOID"] = adj_county["County GEOID"].astype("int")
adj_county.drop(adj_county[round(adj_county['County GEOID'], -3) != round(adj_county["Neighbor GEOID"], -3) ].index, inplace = True) 
all_fips = pd.read_csv("./calculation_data/US_FIPS_Codes.csv", usecols = ["COUNTY_FIPS"])
all_fips.drop(0, inplace = True)
all_fips["COUNTY_FIPS"] = all_fips["COUNTY_FIPS"].astype(int)
disjoint = list(map(int,list(set(all_fips["COUNTY_FIPS"]) - set(landprice["County"]))))

def get_avg_price(county_id):
    land_prices = []
    adj_counties = adj_county[adj_county["County GEOID"] == county_id]["Neighbor GEOID"].to_list()
    for adj_id in adj_counties[1:]:
        try:
            land_prices.append(float(landprice.loc[landprice["County"] == adj_id, "Price (1/4 acre)"]))
        except TypeError:
            continue
    if not len(land_prices):
        return landprice[landprice["state_fips"] == round(county_id,-3)]["Price (1/4 acre)"].mean()
    else:
        return sum(land_prices) / len(land_prices)

est_landprice = [get_avg_price(i) for i in disjoint]
landprice = landprice.append(pd.DataFrame({"County": disjoint, "Price (1/4 acre)": est_landprice, "state_fips" : list(map(lambda x: round(x,-3),disjoint))}))
landprice = landprice.sort_values(by = ["County"]).reset_index()
landprice.to_csv("./calculation_data/landprices.csv")