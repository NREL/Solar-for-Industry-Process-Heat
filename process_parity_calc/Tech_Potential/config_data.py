# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 16:14:12 2020

@author: wxi
Enduse: ['Conventional Boiler Use', 'CHP and/or Cogeneration Process',
       'Boiler/CHP', 'Process Heating']

Fuel_type = ['Natural_gas', 'Diesel', 'Residual_fuel_oil', 'Coal','Coke_and_breeze', 'Other', 'LPG_NGL']

Naics_code = [311111, 311119, 311213, 311221, 311224, 311225, 311230, 311313,
       311314, 311352, 311411, 311412, 311421, 311422, 311423, 311511,
       311512, 311513, 311514, 311520, 311611, 311612, 311613, 311615,
       311710, 311821, 311911, 311919, 311920, 311941, 311942, 311991,
       311999, 312111, 312112, 312113, 312120, 312130, 312140, 312230,
       313110, 313210, 313220, 313230, 313240, 313310, 313320, 321113,
       321211, 321212, 321219, 321911, 321912, 321918, 321920, 321991,
       321992, 321999, 322110, 322121, 322122, 322130, 322219, 322220,
       322291, 322299, 324110, 324121, 324122, 324191, 324199, 325110,
       325120, 325130, 325180, 325193, 325194, 325199, 325211, 325212,
       325220, 325311, 325320, 325411, 325412, 325414, 325510, 325611,
       325612, 325613, 325920, 325992, 325998, 327211, 327213, 332111,
       332112]

The items in the list represent the combinations you are interested in. 

The map will automatically aggregate total tech potential in each county among
the items selected.

Edit the config dictionary entries to meet your needs/requirements. 
"""
config = {
          'enduse': ['Conventional Boiler Use', 'CHP and/or Cogeneration Process',
                     'Boiler/CHP', 'Process Heating'],

          'ft' : ['Natural_gas', 'Diesel', 'Residual_fuel_oil', 'Coal',
                  'Coke_and_breeze', 'Other', 'LPG_NGL'], 

          'naics' : [311111, 311119, 311213, 311221, 311224, 311225, 311230, 311313,
                     311314, 311352, 311411, 311412, 311421, 311422, 311423, 311511,
               311512, 311513, 311514, 311520, 311611, 311612, 311613, 311615,
               311710, 311821, 311911, 311919, 311920, 311941, 311942, 311991,
               311999, 312111, 312112, 312113, 312120, 312130, 312140, 312230,
               313110, 313210, 313220, 313230, 313240, 313310, 313320, 321113,
               321211, 321212, 321219, 321911, 321912, 321918, 321920, 321991,
               321992, 321999, 322110, 322121, 322122, 322130, 322219, 322220,
               322291, 322299, 324110, 324121, 324122, 324191, 324199, 325110,
               325120, 325130, 325180, 325193, 325194, 325199, 325211, 325212,
               325220, 325311, 325320, 325411, 325412, 325414, 325510, 325611,
               325612, 325613, 325920, 325992, 325998, 327211, 327213, 332111,
               332112],
                     
           # your working directory - all files below should be in this directory
           'path' : './calculation_data',
           
           # the tech potential combined csv data file name
           'csvname' : 'tech_jingyi.csv',
           
           # html map file name
           'savename' : 'tech_map.html',
           
           # geodata json file
           'jsonname' : 'tech_pot.json',
           
           # Chart name for map
           'chartname': 'Tech Potential USA',
           
           # Map tech potential color scheme
           'colorscheme': 'yelloworangered',
           
           # path of your shape file- downloadable at: 
           # https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-county-and-equivalent-national-shapefile
           'shapefilepath': 'tl_2017_us_county/tl_2017_us_county.shp'
           
         }



