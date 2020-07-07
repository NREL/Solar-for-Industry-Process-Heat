# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 11:25:54 2020

@author: wxi
"""
# hdf5 file processing imports
import pandas as pd
import numpy as np
import h5py

# Colin mapping file imports
import json
import matplotlib.pyplot as plt
import geopandas as gpd
import requests
import zipfile
from io import BytesIO
from mpl_toolkits.axes_grid1 import make_axes_locatable
import re
import mapclassify

# Configuration file
import tech_opp_config as cfg

def hdf5_to_df(filepath):

    data = h5py.File(filepath , "r")
    df_list = []
    for key in list(data.keys())[:2]:
        df_list.append(pd.DataFrame(np.array(data[key]).squeeze()))
    df = pd.concat(df_list, axis = 1, sort = False)
    df.rename({0: 'industries'}, axis = 'columns', inplace = True)
    df_size = len(df)
    
    for op_hour in ['ophours_high', 'ophours_low', 'ophours_mean']:
        fuels = []
        if "Coal" in data[op_hour].keys():
            fuels.append("Coal")
        if "Natural_gas" in data[op_hour].keys():
            fuels.append("Natural_gas")
        for fuel in fuels:
            fuel_size = len(np.array(data[op_hour][fuel]).T)
            if fuel_size != df_size:
                print(fuel + " isn't complete for each county")
                continue
            df[op_hour + "_" + fuel] = list(np.array(data[op_hour][fuel]).T)
            
        df[op_hour + "_" + "land_use"] = np.array(data[op_hour]['land_use'])
        
        tech_opp = np.array(data[op_hour]['tech_opp']).reshape(3,df_size)
    
        for i in range(3):
            df[op_hour + "_" + str(i)] = tech_opp[i]
    return df

class mapping:

    def __init__(self, data, config):
        
        self.config = config
        
        self.data = data

        cshp_file = config["cfp"]

        sshp_file = config["sfp"]

        self.cshp = gpd.read_file(cshp_file)

        self.sshp = gpd.read_file(sshp_file)

        #Convert to Mercator
        self.cshp = self.cshp.to_crs(epsg=3395)
        self.sshp = self.sshp.to_crs(epsg=3395)

        cb_url = 'http://colorbrewer2.org/export/colorbrewer.json'

        # Updated. Getting SSLError
        try:
            cb_r = requests.get(cb_url)

        except requests.exceptions.SSLError as e:

            print('Exception: {}.\n Using local json file.'.format(e))

            with open(config['cb']) as json_file:
                self.colors = json.load(json_file)

        else:

            self.colors = cb_r.json()

        #  to convert to HEX
        for c in self.colors.keys():

            for n in self.colors[c].keys():

                rgb_list = self.colors[c][n]

                if type(rgb_list) == str:

                    continue

                hex_list = []

                for v in rgb_list:

                    hex = tuple(
                        int(x) for x in re.search(
                            '([^a-z,(,](\w+,\w+,\w+)|(\w,\w+,\w+))', v
                            ).group().split(',')
                        )

                    hex_list.append(hex)

                self.colors[c][n] = hex_list

    def make_county_choropleth(self, data_column, palette, filename,
                               class_scheme,scheme_kwds):
        """
        Class_scheme and scheme_kwds is scheme provided by mapclassify
        (e.g. ‘box_plot’, ‘equal_interval’, ‘fisher_jenks’, etc.). See
        https://pysal.readthedocs.io/en/1.5/library/esda/mapclassify.html for
        more details on both schemes and their keywords.
        """

        #import energy results if path


        def format_county_fips(cf):

            cf = str(int(cf))

            if len(cf)<=4:

                cf = '0'+cf

            return cf


        self.data['COUNTY_FIPS'] = self.data.COUNTY_FIPS.apply(
            lambda x: format_county_fips(x)
            )

        # match on geo_id
        map_data = self.cshp.set_index('GEOID').join(
            self.data.set_index('COUNTY_FIPS')[data_column]
            )

        map_data.dropna(subset=[data_column], inplace=True)

        # set the range for the choropleth
        #vmin, vmax = map_data.MMBtu_TOTAL.min(), map_data.MMBtu_TOTAL.max()

        # create figure and axes for Matplotlib
        fig, ax = plt.subplots(1, figsize=(10, 10))

        # divider = make_axes_locatable(ax)
        #
        # cax = divider.append_axes("right", size="5%", pad=0.1)

        if scheme_kwds == None:

            map_data.plot(column=data_column, cmap=palette, linewidth=0.8,
                          ax=ax, edgecolor='0.8', scheme=class_scheme,
                          legend=True,
                          legend_kwds={'title':'Tech_Opp',
                                       'loc': 'lower right',
                                       'fontsize': 'small'})
        else:

            map_data.plot(column=data_column, cmap=palette, linewidth=0.8,
                          ax=ax, edgecolor='0.8', scheme=class_scheme,
                          classification_kwds=scheme_kwds, legend=True,
                          legend_kwds={'title':'Tech_Opp',
                                       'loc': 'lower right',
                                       'fontsize': 'small'})

        ax.axis('off')


        fig.savefig(filename+'.svg', dpi=500, bbox_inches='tight')

        plt.close()


if __name__ == "__main__":
    
    df = hdf5_to_df(cfg.config["filepath"])

    # Make maps based on any data column in df file
    mm = mapping(df, cfg.config)
    mm.make_county_choropleth(
        cfg.config["data"], palette='YlOrRd', filename=cfg.config["filename"],
        class_scheme='Percentiles', scheme_kwds=None
        )
