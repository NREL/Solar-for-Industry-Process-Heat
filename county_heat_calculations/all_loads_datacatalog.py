 #!/usr/bin/python
import pandas as pd
import shift
import tarfile
import datetime as dt
import make_load_curve
import itertools as it
import multiprocessing

lc_methods = make_load_curve.load_curve()

def calc_all_loads(id):

    loads = lc_methods.calc_load_shape(id[0], id[1]).reset_index()

    return loads

if __name__ == '__main__':

    date = dt.datetime.now().strftime('%Y%m%d')

    lc_methods = make_load_curve.load_curve()

    qpc_naics = lc_methods.swh.NAICS.unique()

    no_good_naics = [51111, 51112, 51113, 51114, 51119]

    qpc_naics = set.difference(set(qpc_naics), set(no_good_naics))

    all_ids = it.product(qpc_naics, lc_methods.emp_size_adj.columns[2:])

    all_loads = pd.DataFrame()

    with multiprocessing.Pool() as pool:

        results = pool.starmap(lc_methods.calc_load_shape, all_ids)

        for ar in results:

            all_loads = all_loads.append(
                    pd.Series(ar), ignore_index=True
                    )

    all_loads.columns = ['month', 'dayofweek', 'hour', 'daily_hours',
                         'fraction_annual_energy','schedule_type_high',
                         'schedule_type_low', 'schedule_type_mean',
                         'NAICS', 'emp_size']

    all_loads.to_parquet(
        'c:/Users/cmcmilla/solar-for-industry-process-heat/results/' +\
        'all_heat_loadshapes_'+date, engine='pyarrow', partition_cols='NAICS'
        )

    print('complete')
