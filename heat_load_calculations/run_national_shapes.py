
import multiprocessing
import os
import make_load_curve
import pandas as pd
from functools import partial


# import county heat data (options to specify temp cutoff, industry, county, end use)


if __name__ == "__main__":

    __spec__ = None

    lc = make_load_curve.load_curve()

    naics_emp = pd.read_parquet(
        'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
        'mfg_eu_temps_20191031_2322.parquet.gzip',
        columns=['naics', 'Emp_Size']
        )

    naics_emp = tuple(naics_emp.drop_duplicates().values)

    with multiprocessing.Pool() as pool:

        results = pool.starmap(
            partial(ls.calc_load_shape, enduse_turndown={'boiler': 4}),
            naics_emp
            )

        for ar in results:

            boiler_ls = pd.concat(
                [pd.Series(ar) for ar in results], axis=0, ignore_index=True
                )

    with multiprocessing.Pool() as pool:

        results = pool.starmap(
            partial(ls.calc_load_shape, enduse_turndown={'process_heat': 5}),
            naics_emp
            )

        for ar in results:

            ph_ls = pd.concat(
                [pd.Series(ar) for ar in results], axis=0, ignore_index=True
                )

    def results_formatting(pool_ls, naics_emp, enduse):

        pool_ls = pd.concat(
            [pool_ls, pd.DataFrame(naics_emp, columns=['naics', 'Emp_Size'])],
            axis=1
            )

        pool_ls['enduse'] = enduse

        pool_ls.to_csv('../results/all_load_shapes_'+enduse+'.csv',
                       compression='gzip')

    results_formatting(boiler_ls, naics_emp, 'boiler')

    results_formatting(ph_ls, naics_emp, 'process_heat')
