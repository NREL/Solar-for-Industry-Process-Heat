#!/usr/bin/env python

import pandas as pd
import format_rev_output
import numpy as np

solar_techs = ['ptc_tes', 'ptc_notes', 'dsg_lf', 'pv_ac', 'pv_dc','swh']

def make_rev_fpath(solar_tech):

    solar_tech1, solar_tech2 = solar_tech, solar_tech

    if solar_tech == 'ptc_tes':

        solar_tech2 = 'ptc_tes6hr'

    if 'pv' in solar_tech:

        solar_tech1, solar_tech2 = 'pv', 'pv'

    rev_fpath = \
        'c:/users/cmcmilla/desktop/rev_output/{}/{}_sc0_t0_or0_d0_gen_2014.h5'.format(
            solar_tech1, solar_tech2
        )

    return rev_fpath

all_gen = pd.DataFrame()

for st in solar_techs:

    rev_fpath = make_rev_fpath(st)

    rev_output = format_rev_output.rev_postprocessing(rev_fpath, st)

    # Available land area in km2, indexed by county_fips
    avail_land = rev_output.area_avail['County Available area km2']

    # System footprint in m2; convert to km2
    sys_footprint = rev_output.generation_group['footprint']/1000**2

    # Calculate number of system multiples given available land area, rounding
    # to nearest integer
    sys_multiple = avail_land.apply(lambda x: np.round(x/sys_footprint,0))

    # Generation in kW; convert to GWh/km2
    annual_gen = pd.DataFrame(rev_output.gen_kW.sum()/10**6)

    annual_gen = annual_gen.join(
        rev_output.county_info.reset_index().set_index(
            'h5_index'
            )['COUNTY_FIPS'], how='left'
        )

    # Scale to availble land area
    annual_gen = annual_gen.set_index('COUNTY_FIPS')[0].multiply(avail_land)

    annual_gen.name = st

    all_gen = pd.concat([all_gen, annual_gen], axis=1, ignore_index=False)

all_gen.to_csv('../Results analysis/mapping/rev_output_for_mapping.csv',
               header=True, index=True)
