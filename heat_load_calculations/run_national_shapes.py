
import multiprocessing
import os
import make_load_curve
import pandas as pd
from functools import partial
import logging
import numpy as np

if __name__ == "__main__":

    __spec__ = None

    # set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('national_shapes.log', mode='w+')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    ls = make_load_curve.load_curve()

    naics_emp = pd.read_parquet(
        'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
        'mfg_eu_temps_20191031_2322.parquet.gzip',
        columns=['naics', 'Emp_Size']
        )

    naics_emp = tuple(naics_emp.drop_duplicates().values)

    logger.info('Starting boiler shapes pool')
    with multiprocessing.Pool() as pool:

        results = pool.starmap(
            partial(ls.calc_load_shape, enduse_turndown={'boiler': 4},
                    hours='qpc', energy='heat'),
            naics_emp
            )

        boiler_ls = pd.concat([df for df in results], ignore_index=True,
                              axis=0)

    logger.info('Boiler pool done.')

    logger.info('Starting process heat shapes pool')
    with multiprocessing.Pool() as pool:

        results = pool.starmap(
            partial(ls.calc_load_shape, enduse_turndown={'process_heat': 5},
                    hours='qpc', energy='heat'),
            naics_emp
            )

        ph_ls = pd.concat([df for df in results], ignore_index=True,
                              axis=0)

    logger.info('Process heat pool done.')

    def results_formatting(pool_ls, naics_emp, enduse):

        pool_ls = pd.concat(
            [pool_ls, pd.DataFrame(np.repeat(naics_emp, 2016, axis=0),
                                   columns=['naics', 'Emp_Size'])],axis=1
            )

        pool_ls['enduse'] = enduse

        pool_ls.to_csv('../results/all_load_shapes_'+enduse+'.gzip',
                       compression='gzip', index=False)

    logger.info('Formatting boiler results')
    results_formatting(boiler_ls, naics_emp, 'boiler')

    logger.info('Formatting process heat results')
    results_formatting(ph_ls, naics_emp, 'process_heat')

    logger.info('Shapes complete.')
    print('Shapes complete.')
