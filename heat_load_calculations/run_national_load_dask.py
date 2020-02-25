import dask.dataframe as dd
import pandas as pd


county_energy = pd.read_parquet(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
    'mfg_eu_temps_20191031_2322.parquet.gzip'
    )

county_ind_size_temp_total = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'Temp_C']
    ).MMBtu.sum()

boiler_energy = county_ind_size_temp_total[
    (county_ind_size_temp_total.End_use == 'CHP and/or Cogeneration Process') |
    (county_ind_size_temp_total.End_use == 'Conventional Boiler Use')
    ].copy(deep=True).set_index(['naics', 'Emp_Size'])

ph_energy = county_ind_size_temp_total[
    county_ind_size_temp_total.End_use == 'Process Heating'
    ].copy(deep=True).set_index(['naics', 'Emp_Size'])

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

# Merge boiler-ls and ph_ls with blank_8760 before merging with energy dd

dd_boiler_energy = dd.merge(boiler_energy, boiler_ls, left_index=True,
                            right_index=True)

dd_ph_energy = dd.merge(ph_energy, ph_ls, left_index=True, right_index=True)




# Calculate fuel mix by county, industry, size, and end use
fuel_mix_enduse = county_energy.groupby(
    ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT']
    ).MMBtu.sum().divide(county_ind_size_temp_total.sum(level=[0,1,2,3]))



blank_8760 =

    annual_energy = ph_or_boiler_energy_df.xs(county, level=0)

    load_8760 = pd.DataFrame(annual_energy).join(ph_or_boiler_ls)

    load_8760 = pd.merge(
        load_8760_blank.reset_index(), load_8760.reset_index(),
        on=['month', 'dayofweek', 'hour'], how='left'
        ).set_index(['naics', 'Emp_Size'])

    load_factor = load_8760[['Weekly_op_hours','Weekly_op_hours_low',
                             'Weekly_op_hours_high']].mean(level=[0,1])

# sum everything up by county_fips, naics, end_use, temp.
