#!/usr/bin/env python
import h5py
import numpy as np
import datetime
import itertools

def create_h5(target_file_path, tech_opp_results):
    """
    Creates and saves h5 file of tech opportunity results.
    """
    # tech_opp_results as a list of numpy arrays:
    # [tech_opp_meta (1,), time_index (8760,1), names (3,1),
    #  county_tech_opp (3, 8760)(8760,1),
    #  county_tech_opp_fuels(3, 3106)(8760, n), county_tech_opp_land (3,1),
    #  county_total_load(3, 8760)]

    # Range of average weekly operating hours
    ophours = ['ophours_mean', 'ophours_high', 'ophours_low']

    len_results = len(tech_opp_results)

    all_meta = np.vstack([a[0] for a in tech_opp_results])

    time_index = tech_opp_results[0][1]

    time_index = time_index.strftime('%Y-%m-%d %T').values

    time_index = time_index.astype('S')

    fuels = list(tech_opp_results[0][4][0].dtype.names)

    op_list = [f for f in fuels]
    op_list.append('tech_opp')

    ophours_index = {}

    tech_opp = {}

    tech_opp_land = {}

    total_load = {}

    for op_h in ophours:

        ophours_index[op_h] = \
            np.where(tech_opp_results[0][2]==op_h)[0][0]

        tech_opp[op_h] = {d: {} for d in op_list}

        tech_opp[op_h]['tech_opp'] = np.stack(
            [a[3][:,ophours_index[op_h]] for a in tech_opp_results], axis=1
            )

        total_load[op_h] = {d: {} for d in op_list}

        total_load[op_h]['total_load'] = np.stack(
            [a[6][:,ophours_index[op_h]] for a in tech_opp_results], axis=1
            )

        for fuel in fuels:

            mask = \
                [1 if fuel in a[4].dtype.names else 0 for a in tech_opp_results]

            matching = itertools.compress([a[4] for a in tech_opp_results],
                                          mask)

            tech_opp[op_h][fuel] = np.stack(
                [a[fuel][ophours_index[op_h]] for a in matching], axis=1
                )

        tech_opp_land[op_h] = np.hstack(
            [a[5][ophours_index[op_h]] for a in tech_opp_results]
            )

    f = h5py.File(target_file_path, 'w')

    f.attrs.create('timestamp',
                    datetime.datetime.today().strftime('%Y%m%d_%H%M'))
    f.attrs.create('h5py_version', h5py.version.version)

    # Include datetime
    f.create_dataset('time_index', data=time_index, dtype='S19')

    for op_h in ophours:

        f.create_dataset(op_h+'/tech_opp', data=tech_opp[op_h]['tech_opp'])
        f.create_dataset(op_h+'/total_load', data=total_load[op_h]['total_load'])
        f[op_h+'/total_load'].attrs.create('units', 'MW')
        f.create_dataset(op_h+'/land_use', data=tech_opp_land[op_h])
        f[op_h+'/land_use'].attrs.create('units', 'km2')

        for fuel in fuels:

            f.create_dataset(op_h+'/'+fuel, data=tech_opp[op_h][fuel])
            f[op_h+'/'+fuel].attrs.create('desc',
                                          '% of {} displaced'.format(fuel))

    f['ophours_mean'].attrs.create('desc', 'mean weekly operating hours')
    f['ophours_low'].attrs.create('desc','low weekly operating hours')
    f['ophours_high'].attrs.create('desc','high weekly operating hours')

    # Write county info
    f.create_dataset(
        'county_info',
        data=all_meta[['COUNTY_FIPS', 'avail_land', 'timezone']].T
        )
    f['county_info'].attrs.create(
        'desc', 'County FIPS, available land area (km2), and timezone'
        )

    # Special formatting for lists in array
    string_dt = h5py.special_dtype(vlen=str)

    f.create_dataset("industries", data=all_meta['industries'].T,
                     dtype=string_dt)

    f.close()

    print('File Saved')

    return
