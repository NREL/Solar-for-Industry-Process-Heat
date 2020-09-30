import os
import sys
import time
import multiprocessing
import pickle
import pyarrow as pa
import numpy as np
import pandas as pd
from functools import partial
import dask.dataframe as dd
from tech_opp_demand import demand_results
sys.path.append('../')
from heat_load_calculations.run_demand_8760 import demand_hourly_load

# Parameters for running tech opportunity
data_dir = 'c:/users/cmcmilla/desktop/'
# tech_package = 'ptc_notes'
sizing_month = 6

# Dictionary by tech package of all solar gen and process energy inputs
tech_opp_inputs = {
    'dsg_lf': {'supply': '/dsg_lf/dsg_lf_sc0_t0_or0_d0_gen_2014.h5',
               'demand': 'LF_process_energy.csv.gz'},
    'swh': {'supply': '/swh/swh_sc0_t0_or0_d0_gen_2014.h5',
            'demand': 'fpc_hw_process_energy.csv.gz'},
    'pv_boiler': {'supply': '/pv/pv_sc0_t0_or0_d0_gen_2014.h5',
                  'demand': 'eboiler_process_energy.csv.gz'},
    'pv_resist': {'supply': '/pv/pv_sc0_t0_or0_d0_gen_2014.h5',
                  'demand': 'resistance_process_energy.csv.gz'},
    'pv_whrhp': {'supply': '/pv/pv_sc0_t0_or0_d0_gen_2014.h5',
                 'demand': 'whr_hp_process_energy.csv.gz'},
    'ptc_tes': {'supply': '/ptc_tes/ptc_tes6hr_sc0_t0_or0_d0_gen_2014.h5',
                'demand': 'ptc_process_energy.csv.gz'},
    'ptc_notes': {'supply': '/ptc_notes/ptc_notes_sc0_t0_or0_d0_gen_2014.h5',
                  'demand': 'ptc_process_energy.csv.gz'}
    }

# def stream_parquet(county_8760):
#     """ """
#     context = pa.default_serialization_context()
#     serialized_df = context.serialize(county_8760)
#     df_components = serialized_df.to_components()


def aggr_county_8760(county, county_8760):
    """Aggregates hourly county heat demand (in MW) by 3-digit NAICS code"""

    county_8760 = county_8760.sum(level=[0, 3, 4])
    county_8760.reset_index(inplace=True)
    county_naics = pd.DataFrame(county_8760.naics.unique(), columns=['naics'])
    county_naics['naics_3'] = \
        county_naics.naics.apply(lambda x: int(str(x)[0:3]))

    county_8760 = pd.merge(county_8760, county_naics, on='naics', how='left')

    county_8760['county_fips'] = county

    county_8760 = county_8760.groupby(['county_fips', 'naics_3', 'index',
                                       'op_hours'], as_index=False).MW.sum()

    # There may be 0 or nan values that remain in the process demand data set.
    zero_mw = county_8760.groupby(['county_fips', 'naics_3']).MW.sum()
    county_8760 = pd.merge(county_8760,
                           zero_mw.where(zero_mw != 0).dropna(),
                           left_on=['county_fips', 'naics_3'],
                           right_index=True, how='inner',
                           suffixes=['', '_zero'])

    county_8760.drop(['MW_zero'], axis=1, inplace=True)

    return county_8760


def calc_county(county, hourly_demand):
    # Calculate hourly load for annual demand (in MW). Calculate December and
    # June demand (in MWh) for sizing generation
    print('County ', county)

    county_8760, county_peak = \
        hourly_demand.calculate_county_8760_and_peak(
            county, peak_month=6, peak_MW=False
            )

    county_8760 = aggr_county_8760(county, county_8760)

    return county_8760


if __name__ == "__main__":

    __spec__ = None

    for tech_package in tech_opp_inputs.keys():
        if tech_package != 'ptc_tes':
            continue
        else:
            demand_filepath = tech_opp_inputs[tech_package]['demand']
            # Time stamp (in UTC) for  file.
            time_stamp = time.strftime('%Y%m%d_%H%M', time.gmtime())
            pickle_results = False

            # Start timing calculations
            start = time.time()

            # Instantiate process demand data (annual MMBtu) and methods
            process_demand = demand_results(os.path.join(data_dir,
                                                         demand_filepath))

            # Instantiate methods for creating hourly loads (in MW)
            hourly_demand = demand_hourly_load(2014,
                                               process_demand.demand_data)
            counties = tuple(process_demand.demand_data.COUNTY_FIPS.unique())

            partial_county = partial(calc_county, hourly_demand=hourly_demand)
            # Run calculations in parallel
            with multiprocessing.Pool(processes=8) as pool:
                results = pool.map(partial_county, counties)

            if tech_package == 'ptc_tes':
                tech_package = 'ptc'

            # Save as parquet file
            results = pd.concat([x for x in results], axis=0,
                                ignore_index=True)

            for op_h in ['ophours_mean', 'ophours_low', 'ophours_high']:
                target_file_path = '{}_process_demand_8760_{}_{}'.format(
                        tech_package, op_h, time_stamp
                                )
                query_op = "op_hours == '{}'".format(op_h)

                try:
                    results.query(query_op).to_parquet(
                        target_file_path, index=False,
                        partition_cols=['naics_3'],
                        compression='gzip', engine='pyarrow'
                        )
                except MemoryError as error:
                    print("Error:{}. Switching to Dask".format(error))
                    dd_results = dd.from_pandas(results, npartitions=500)
                    dd_results.to_parquet(
                        target_file_path, engine='pyarrow', compression='gzip',
                        partition_on=['naics_3'], compute=True
                    )

    # End timer
    end = (time.time()-start)
    print('---------')
    print("it took {} seconds to run.".format(np.round(end, 0)))
    print('---------')
