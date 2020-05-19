
import pandas as pd
from ghg import Emissions
import pytest
import os


ghg_calcs = Emissions(2014)

def test_ghgrp_calcs_data():

    # delete data

    ei_file = './calculation_data/ghgrp_CO2e_intensity_2014.csv'

    fd_file = './calculation_data/ghgrp_fuel_disagg_2014.csv'

    for f in [ei_file, fd_file]:

        if os.path.exists(f):

            os.remove(f)

        else:

            continue

    # run
    final_ghgrp_CO2e_intensity, final_ghgrp_fuel_disagg = \
        ghg_calcs.calc_ghgrp_and_fuel_intensity()

    ei_exists = os.path.exists(ei_file)

    fd_exists = os.path.exists(fd_file)

    assert (ei_exists & fd_exists)



# Other tests: back out energy from emissions, asserting against ghgrp calcs
# for non-GHGRP data
