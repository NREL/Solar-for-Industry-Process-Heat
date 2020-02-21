import multiprocessing
import os
import make_load_curve
import pandas as pd
import logging
import numpy as np

import dask


# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('national_load.log', mode='w+')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
handler.setFormatter(formatter)
logger.addHandler(handler)


lc = make_load_curve()

calc_annual_load(annual_energy, load_shape)

# Import county energy
county_energy = pd.read_parquet(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
    'mfg_eu_temps_20191031_2322.parquet.gzip'
    )

# Import load shapes (defined by naics and employment size class)
boiler_ls = pd.read_csv(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
    'all_load_shapes_boiler.gzip', compression='gzip',
    index_col=['naics', 'Emp_Size']
    )

ph_ls = pd.read_csv(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
    'all_load_shapes_process_heat.gzip', compression='gzip',
    index_col=['naics', 'Emp_Size']
    )
# ['COUNTY_FIPS', 'Emp_Size', 'MECS_FT', 'MECS_Region', 'data_source',
       # 'est_count', 'fipstate', 'naics', 'End_use', 'Temp_C', 'MMBtu'],

# Calculate county energy total by industry, size, end use, and temperaure
county_ind_size_temp_total = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'Temp_C']
    ).MMBtu.sum()

# Calculate fuel mix by county, industry, size, and end use
fuel_mix_enduse = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT']
    ).MMBtu.sum().divide(county_ind_size_temp_total.sum(level=[0,1,2,3]))

# Get all county, industry, naics, empsize, and temperature combinations
ls_archetypes = county_energy[['naics', 'Emp_Size']].drop_duplicates().values

def select_load_shape(naics, empsize, enduse):

    if end_use in ['CHP and/or Cogeneration Process','Conventional Boiler Use']:

        ls = boiler_ls

    if end_use == 'Process_heat':

        ls = ph_ls

    ls = ls.xs([naics, empsize]).drop('enduse', axis=1)

    return ls

def select_annual_energy(naics, empsize, enduse):

    energy = county_ind_size_temp_total.xs(
        [naics, empsize, enduse], level=['naics', 'Emp_Size', 'End_use']
        ).sum(level=0)

    return energy



for cc in ls_archetypes:

    ls = dask.delayed(select_load_shape)(cc)

    energy = dask.delayed(select_annual_energy)(cc)



# Build inputs


# Want peak load by county, so first sum by fuel, temp end use
# calculate max load hour by each county (convert to MW)

with multiprocessing.Pool() as pool:

    results = pool.starmap(

        )

    boiler_ls = pd.concat([df for df in results], ignore_index=True,
                          axis=0)

# Tract projections are made at the total household level and are not
# disaggregated by hh type. This can be changed in the future, but
# increased computational resources are needed.
if rf.geo == 'tract':

    states_geos = pd.DataFrame(rf.res_agg_dict['total']['estimate'])

    states_geos['df'] = 'total'

    states_geos.reset_index(inplace=True)

    states_geos.drop_duplicates(['state', 'geoid'], inplace=True)

    states_geos = tuple(states_geos[['df', 'state', 'geoid']].values)

else:

    states_geos = pd.concat(
        [rf.res_agg_dict[k]['estimate'] for k in rf.res_agg_dict.keys()],
        axis=1, ignore_index=False
        )

    states_geos = states_geos.reset_index().groupby(
            ['state', rf.geo]
            )[['single', 'multi', 'mobile']].sum()

    states_geos = states_geos.reset_index().melt(id_vars=['state', rf.geo],
                                         value_name='hh', var_name='df')

    states_geos = pd.DataFrame(states_geos[states_geos.hh !=0])

    states_geos = tuple(states_geos[['df', 'state', rf.geo]].values)

calc_load_shape(self, naics, emp_size, enduse_turndown={'boiler': 4},
                hours='qpc', energy='heat'):

res_forecasts = pd.DataFrame()

with multiprocessing.Pool() as pool:

    results = pool.starmap(rf.run_forecast_parallel, states_geos)

    for ar in results:

        res_forecasts = res_forecasts.append(
                pd.Series(ar), ignore_index=True
                )

print('complete')

res_forecasts.to_csv(ff_dict[rf.geo], compression='gzip',
                     index=False)


if ('res_forecasts_tract' in os.listdir()):

    forecasts = pd.read_csv('res_forecasts_tract', compression='gzip')

    rf.calc_final_projections(forecasts)
