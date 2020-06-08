# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 18:16:02 2020

@author: wxi
"""

import pandas as pd
import geopandas as gpd
import altair as alt
import config_data as cd
import os
import json

class TechPlot:
    def __init__(self, config):
        '''Creates data for Plotting'''
        # establish file paths
        self.config = config
        path = self.config['path']
        self.csvpath = os.path.join(path, self.config['csvname'])
        self.savename = os.path.join(path, self.config['savename'])
        self.jsonpath = os.path.join(path, self.config['jsonname'])

        # shape file path
        self.shp = os.path.join(path, self.config['shapefilepath'])
        
        # Preprocess data
        data = pd.read_csv(self.csvpath)
        data["End_use"].replace("Process heating", "Process Heating", inplace = True)
        data["COUNTY_FIPS"] = data["COUNTY_FIPS"].astype(str)
        
        # Filter
        m_naics = data["naics"].isin(self.config['naics'])
        m_enduse = data["End_use"].isin(self.config['enduse'])
        m_ft = data["MECS_FT"].isin(self.config['ft'])
        filt_data = data[m_naics & m_enduse & m_ft]
        
        # Groupby -> DataFrame
        self.gb = pd.DataFrame(filt_data.groupby(["COUNTY_FIPS"])["tech_potential"].sum()).reset_index()
        
    def create_gdp(self):
        '''Creates json file for Altair'''
        # Read File
        gdf = gpd.read_file(self.shp)
        
        # Preprocess
        gdf["FIPS"] = gdf["STATEFP"].astype(str) + gdf["COUNTYFP"].astype(str)
        gdf["FIPS"] = gdf["FIPS"].apply(lambda x : x[1:] if x.startswith("0") else x)
        
        # Merge DataFrames
        gdfmerged = gdf.merge(self.gb, how = "left", left_on = "FIPS", right_on = "COUNTY_FIPS")
        
        # Postprocess
        gdfmerged.drop(["AWATER", "ALAND", "CBSAFP", "CLASSFP", "COUNTYNS", "CSAFP", "FUNCSTAT", "INTPTLAT", "INTPTLON", "LSAD", "METDIVFP", "MTFCC", "NAME", "GEOID", "COUNTY_FIPS"], axis = 1, inplace= True)
        excl_state = ['02', '66', '72', '15', '69', '78', '60']
        gdfmerged = gdfmerged[~gdfmerged["STATEFP"].isin(excl_state)]
        gdfmerged.drop(["STATEFP", "COUNTYFP"], axis = 1, inplace= True)
        
        print("Exporting Json File....")
        # Export json file to working directory
        gdfmerged.to_file(self.jsonpath, driver="GeoJSON")
        
        print("Export Done")

    def save_plot(self):
        """save a html plot in working directory"""
        
        "plot function in Altair"
        def gen_techpot_map(geodata, color_column, title, tooltip, color_scheme='yelloworangered'):
    
            base = alt.Chart(geodata, title = title).mark_geoshape(
                stroke='black',
                strokeWidth=1
            ).encode(
            ).properties(
                width=800,
                height=800
            )

            choro = alt.Chart(geodata).mark_geoshape(
                stroke='black'
            ).encode(
                color = alt.Color(color_column, 
                          type='quantitative', 
                          scale=alt.Scale(scheme=color_scheme),
                          title = title),
                tooltip = tooltip
            )
            return base + choro

        # Load json file
        print("Loading json file...")
        with open(self.jsonpath) as json_file:
            tech_json = json.load(json_file)
            
        # Extract data from json file
        tech_data = alt.Data(values=tech_json['features'])
        
        # Plot map
        print("Creating county map...")
        tech_map = gen_techpot_map(tech_data, color_column='properties.tech_potential:Q', 
                                   title = self.config["chartname"], color_scheme= self.config["colorscheme"] , 
                                   tooltip=['properties.NAMELSAD:O', 'properties.tech_potential:Q'])
        
        # Save map
        print("Saving county map...")
        tech_map.save(self.savename)

        print("Map created!")

if __name__ == "__main__":
    plotobj = TechPlot(cd.config)
    plotobj.create_gdp()
    plotobj.save_plot()