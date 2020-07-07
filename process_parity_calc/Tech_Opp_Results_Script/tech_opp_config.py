# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 11:26:17 2020

@author: wxi
"""

config = {
          # Relative or absolute file path for your hdf5 file
          'filepath': './calculation_data/dsg_lf_sizing_6_20200623_0134.hdf5',
          # Name of svg file
          'filename': 'tech_opp_results',
          # legend title
          'leg_title': "tech_opp",
          # county shp file path
          'cfp' : './calculation_data/tl_2017_us_county/tl_2017_us_county.shp',
          # state shp file path
          'sfp' : './calculation_data/cb_2018_us_state_500k/cb_2018_us_state_500k.shp',
          # colorbrewer.json file path:
          'cb' : "colorbrewer.json",
          # data column for the map
          "data": "ophours_high_0"
         }



