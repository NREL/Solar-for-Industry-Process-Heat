# Plotting technical potential by county
This folder contains a script to save a html map for a specified technical potential analysis.

## Prerequisites
Altair 4.1.0 - https://altair-viz.github.io/getting_started/installation.html
Geopandas 0.6.1 - https://geopandas.org/install.html

## How to Use

1. Rnsure that all the necessary files (including the .py files in this folder) are in your working directory.
2. Edit the config_data.py file to meet your file locations/filters for tech potential. 
3. Run the tech_plot.py file to save a geopandas json file and an Altair html map file in your working directory.
* The code takes a while to run due to the relatively large county dataset. 