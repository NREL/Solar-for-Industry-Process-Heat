
import h5py
import pandas as pd


def make_demand_h5(tech_package):

    # dictionary of tech packages and their end uses, temp ranges, and
    # industries.
    # Select relevant data, aggregate into MECS region (count of facilities) and GHGRP data
    # Apply NU model to get annual MMBtu
    # Convert to kWh
    # Aggregate to county, naics, emp_size, end_use, and kWh
    # Apply representative load profiles
    # Create h5 file with dataset of kW demand (shape=(8760, 3106)),
    #   county info (FIPS, shape=(3106, 2))

    ## See http://docs.h5py.org/en/stable/high/dataset.html#creating-datasets
    ## for info on creating datasets.
    ## Need to first create file >f = h5py.File('myfile.hdf5','w')
    ## then create group (to then add dataset to?)
