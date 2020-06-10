#!/usr/bin/env python
import h5py
import numpy
import datetime

def create_h5(target_file_path, tech_opp_results):
    """
    Creates and saves h5 file of tech opportunity results.
    """

    f = h5py.File(target_file_path, 'w')
    f.attrs.create('timestamp',
                    datetime.datetime.today().strftime('%Y%m%d_%H%M'))
    f.attrs.create('h5py_version', h5py.version.version)

    county_info = f.create_dataset('county_info',
                                   tech_opp_results['county_info'],
                                   dtype=('i8', 'f'))
    county_info.attrs.create('desc',
                             'County FIPS and available land area (km2)')

    to_mean = f.create_group('ophours_mean')
    to_mean.attrs.create('desc', 'mean weekly operating hours')

    to_low = f.create_group('ophours_low')
    to_low.attrs.create('desc','low weekly operating hours')

    to_high = f.create_group('ophours_high')
    to_high.attrs.create('desc','high weekly operating hours')

    to_mean_ds = to_mean.create_dataset(
        'tech_opp_mean', data=tech_opp_results['ophours_mean']['tech_opp'],
        dtype='f8')

    to_low_ds = to_low.create_dataset(
        'tech_opp_low', data=tech_opp_results['ophours_low']['tech_opp'],
        dtype='f8')

    to_high_ds = to_high.create_dataset(
        'tech_opp_high', data=tech_opp_results['ophours_high']['tech_opp'],
        dtype='f8')

    for group in f.keys():

        for ds in f[group].keys():

            f[group][ds].attrs.create('desc',
                                      'solar gen % of industry demand')

    to_mean_land_abs = to_mean.create_dataset(
        'tech_opp_land_abs',
        data = tech_opp_dict['ophours_mean']['land']['abs'],
        dtype='i8'
        )
    to_mean_land_abs.attrs.create('desc', 'land use')
    to_mean_land.abs.attrs.create('units', 'km2')

    to_mean_land_pct = to_mean.create_dataset(
        'tech_opp_land_pct',
        data = tech_opp_dict['ophours_mean']['land']['pct_of_avail'],
        dtype='i8'
        )
    to_mean_land_pct.attrs.create('desc', 'use of availalbe land')
    to_mean_land_pct.attrs.create('units', '%')

    to_low_land_abs = to_low.create_dataset(
        'tech_opp_land_abs',
        data = tech_opp_dict['ophours_low']['land']['abs'],
        dtype='i8'
        )
    to_low_land_abs.attrs.create('desc', 'use of available land')
    to_low_land.abs.attrs.create('units', 'km2')

    to_low_land_pct = to_low.create_dataset(
        'tech_opp_land_pct',
        data = tech_opp_dict['ophours_low']['land']['pct_of_avail'],
        dtype='i8'
        )
    to_low_land_pct.attrs.create('desc', 'use of availalbe land')
    to_low_land_pct.attrs.create('units', '%')

    to_high_land_abs = to_high.create_dataset(
        'tech_opp_land_abs',
        data = tech_opp_dict['ophours_high']['land']['abs'],
        dtype='i8'
        )
    to_high_land_abs.attrs.create('desc', 'use of available land')
    to_high_land.abs.attrs.create('units', 'km2')

    to_high_land_pct = to_high.create_dataset(
        'tech_opp_land_pct',
        data = tech_opp_dict['ophours_high']['land']['pct_of_avail'],
        dtype='i8'
        )
    to_high_land_pct.attrs.create('desc', 'use of availalbe land')
    to_high_land_pct.attrs.create('units', '%')

    to_mean_ng = to_mean.create_dataset(
        'tech_opp_natural_gas',
        data=tech_opp_results['ophours_mean']['tech_opp_natural_gas'],
        dtype='f8')
    to_mean_ng.attrs.create('desc', '% of natural gas displaced')

    to_mean_coal = to_mean.create_dataset(
        'tech_opp_coal',
        data=tech_opp_results['ophours_mean']['tech_opp_coal'],
        dtype='f8')
    to_mean_coal.attrs.create('desc', '% of coal displaced')

    to_low_ng = to_low.create_dataset(
        'tech_opp_natural_gas',
        data=tech_opp_results['ophours_low']['tech_opp_natural_gas'],
        dtype='f8')
    to_low_ng.attrs.create('desc', '% of natural gas displaced')

    to_low_coal = to_low.create_dataset(
        'tech_opp_coal',
        data=tech_opp_results['ophours_low']['tech_opp_coal'],
        dtype='f8')
    to_low_coal.attrs.create('desc', '% of coal displaced')

    to_high_ng = to_high.create_dataset(
        'tech_opp_natural_gas',
        data=tech_opp_results['ophours_high']['tech_opp_natural_gas'],
        dtype='f8')
    to_high_ng.attrs.create('desc', '% of natural gas displaced')

    to_high_coal = to_high.create_group(
        'tech_opp_coal',
        data=tech_opp_results['ophours_high']['tech_opp_coal'],
        dtype='f8')
    to_high_coal.attrs.create('desc', '% of coal displaced')

    f.close()

    return 'File Saved'
