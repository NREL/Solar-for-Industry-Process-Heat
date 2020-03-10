import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
import pyarrow.parquet as pq

# Have dask data frame that has all load info by naics
# Group by county and perform calculations
# do calculations
# use persist

client = Client()
# client.map(method, input)

boiler_ls_file = \
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
    'all_load_shapes_boiler.gzip'

ph_ls_file =\
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
    'all_load_shapes_process_heat.gzip'

county_energy_file = \
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
    'mfg_eu_temps_20191031_2322.parquet.gzip'

def make_blank_8760(year):

    dtindex = \
        pd.date_range(str(year)+'-01-01', str(year+1)+'-01-01', freq='H')[0:-1]

    load_8760_blank = pd.DataFrame(index=dtindex)

    load_8760_blank['month'] = load_8760_blank.index.month

    load_8760_blank['dayofweek'] = load_8760_blank.index.dayofweek

    load_8760_blank['Q'] = load_8760_blank.index.quarter

    load_8760_blank['hour'] = load_8760_blank.index.hour

    # load_8760_blank['date'] = load_8760_blank.index.date

    return load_8760_blank

load_8760_blank = make_blank_8760(year)

county_energy = pd.read_parquet(county_energy_file, engine='pyarrow')

county_energy.drop(['MECS_FT', 'MECS_Region', 'data_source', 'est_count', 'Q',
                    'fipstate'], axis=1, inplace=True)

county_energy.set_index(['naics'], inplace=True)


dd_county_energy = dd.from_pandas(
    county_energy, npartitions=len(county_energy.index.unique())
    )


county_energy_eu_size = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use'], as_index=False
    ).MMBtu.sum()

county_energy_eu_size.set_index(['naics'], inplace=True)

dd_county_energy = dd.from_pandas(
    county_energy_eu_size,
    npartitions=len(county_energy_eu_size.index.unique())
    )

# county_ind_size_temp_total = county_energy.groupby(
#     ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'Temp_C']
#     ).MMBtu.sum()

# turn lines 15 - 59 into method that creates dask dataframe and load factor
# for boiler and ph
# boiler_energy = county_ind_size_temp_total[
#     (county_ind_size_temp_total.End_use == 'CHP and/or Cogeneration Process') |
#     (county_ind_size_temp_total.End_use == 'Conventional Boiler Use')
#     ].copy(deep=True).set_index(['naics', 'Emp_Size'])
#
# ph_energy = county_ind_size_temp_total[
#     county_ind_size_temp_total.End_use == 'Process Heating'
#     ].copy(deep=True).set_index(['naics', 'Emp_Size'])

# Import load shapes (defined by naics and employment size class)

def import_load_info2dask(enduse):
    """
    Imports and formats load shape. Returns dask dataframe of load shape
    and pandas dataframe of load factor.
    """

    if enduse == 'boiler':

        ls_file = boiler_ls_file

        ls = pd.read_csv(
            ls_file,compression='gzip',index_col=['naics','Emp_Size']
            )

        ls = pd.concat(
            [boiler_ls.replace({'boiler': 'Conventional Boiler Use'}),
             boiler_ls.replace({'boiler': 'CHP and/or Cogeneration Process'})],
             axis=0
            )

    elif enduse == 'process heat':

        ls_file = ph_ls_file

        ls = pd.read_csv(
            ls_file,compression='gzip',index_col=['naics','Emp_Size']
            )

    else:

        print('Wrong enduse')

        return

    ls.rename(columns={'enduse': 'End_use'}, inplace=True)

    # Create 8760 dataframe
    ls = pd.merge(
        load_8760_blank.reset_index(), ls.reset_index(),
        on=['month', 'dayofweek', 'hour'], how='left'
        ).set_index(['naics', 'Emp_Size'])

    ls.drop(['month', 'dayofweek', 'hour'], axis=1, inplace=True)

    ls.rename(columns={'index': 'timestamp'}, inplace=True)

    # Calculate load factor by NAICS and employment size class
    lf = ls[['Weekly_op_hours','Weekly_op_hours_low',
             'Weekly_op_hours_high']].mean(level=[0,1])

    lf = lf.sort_index()

    ls.reset_index('Emp_Size', inplace=True)

    ls = dd.from_pandas(ls, npartitions=len(ls.index.unique()))

    return ls, lf

dd_boiler, lf_boiler = import_load_info2dask('boiler')

dd_ph, lf_ph = import_load_info2dask('process heat')



# boiler_ls = pd.read_csv(
#     'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
#     'all_load_shapes_boiler.gzip', compression='gzip',
#     index_col=['naics', 'Emp_Size']
#     )
#
# boiler_ls = pd.concat(
#     [boiler_ls.replace({'boiler': 'Conventional Boiler Use'}),
#      boiler_ls.replace({'boiler': 'CHP and/or Cogeneration Process'})], axis=0
#     )
#
# boiler_ls.rename(columns={'enduse': 'End_use'}, inplace=True)
#
# ph_ls = pd.read_csv(
#     'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
#     'all_load_shapes_process_heat.gzip', compression='gzip',
#     index_col=['naics', 'Emp_Size']
#     )
#
# ph_ls.rename(columns={'enduse': 'End_use'}, inplace=True)
#
# boiler_ls_8760 = pd.merge(
#     load_8760_blank.reset_index(), boiler_ls.reset_index(),
#     on=['month', 'dayofweek', 'hour'], how='left'
#     ).set_index(['naics', 'Emp_Size'])
#
# lf_boiler_ls = boiler_ls_8760[['Weekly_op_hours','Weekly_op_hours_low',
#                          'Weekly_op_hours_high']].mean(level=[0,1])
#
# lf_boiler_ls = lf_boiler_ls.sort_index()
#
# boiler_ls_8760.reset_index('Emp_Size', inplace=True)

# dd_county_energy = dd.from_pandas(
#     county_energy_eu_size,
#     npartitions=len(county_energy_eu_size.index.unique())
#     )


# dd_boiler = dd.merge(county_energy_eu_size, boiler_ls_8760,
#                      on=['naics', 'Emp_Size'], how='left')


 #df.groupby(['idx', 'x']).apply(myfunc), where idx is the index level name
# dd_boiler.groupby(['naics', 'Emp_Size']).apply(new_func)


def calc_load_dask(group, lf_ls):

    county_time = group[['COUNTY_FIPS', 'index']]

    result = dd.concat(
        [county_time, group[['Weekly_op_hours', 'Weekly_op_hours_low',
                             'Weekly_op_hours_high']].mul(lf_ls, level=[0,1])),
        axis=1, join='inner'
        )

    return result

# def calc_fuel_dask(group, fuel_mix_enduse):
#     """
#     Break out end use load by fuel mix.
#     """
#
# def calc_temp_dask(group, temp_enduse):
#     """
#     Break out
#     """


def results_to_parquet(results)


def select_county(t_dd, county):

    blah = t_dd[t_dd.COUNTY_FIPS == county]

    return blah

def do_stuff(blah):

    new_output = dd.concat(

    )



ph_ls_8760 = pd.merge(
    load_8760_blank.reset_index(), ph_ls.reset_index(),
    on=['month', 'dayofweek', 'hour'], how='left'
    ).set_index(['naics', 'Emp_Size'])

lf_ph_ls = ph_ls_8760[['Weekly_op_hours','Weekly_op_hours_low',
                         'Weekly_op_hours_high']].mean(level=[0,1])

lf_ph_ls = lf_ph_ls.reset_index('Emp_Size').sort_index()

ph_ls_8760.reset_index('Emp_Size', inplace=True)

# Merge boiler-ls and ph_ls with blank_8760 before merging with energy dd
dd_boiler_energy = dd.merge(boiler_energy, boiler_ls, left_index=True,
                            right_index=True)

dd_ph_energy = dd.merge(ph_energy, ph_ls, left_index=True, right_index=True)



# Calculate fuel mix by county, industry, size, and end use
fuel_mix_enduse = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT']
    ).MMBtu.sum().divide(county_ind_size_temp_total.sum(level=[0,1,2,3]))



def load_method(dd, load_factor):

    peak_demand = load_factor**-1

    peak_demand = peak_demand.multiply(annual_energy, axis=0)/8760

    load_8760.set_index('index', append=True, inplace=True)

    load_8760.sort_index(inplace=True)

    load_8760.update(
        load_8760[['Weekly_op_hours','Weekly_op_hours_low',
                   'Weekly_op_hours_high']].multiply(peak_demand)
        )

# sum everything up by county_fips, naics, end_use, temp.
