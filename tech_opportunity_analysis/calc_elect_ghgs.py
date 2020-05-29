# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 16:38:00 2017

@author: vnarwade
revised by cmcmilla
"""

import pandas as pd
import numpy as np
import itertools
import re
import os

class electricity(object):
    """
    Class containing methods used to calculate electricity emission factors
    (MTCO2e/MWh) by county from EPA eGRID data.
    """

    data_wd = './calculation_data'

    fips_to_zip_file = os.path.join(data_wd,'COUNTY_ZIP_032014.csv')

    e_grid_file = \
        os.path.join(data_wd,'power_profiler_zipcode_tool_2014_v7.1_1.xlsx')

    egrid_ef = pd.read_excel(
        e_grid_file, sheet_name='eGRID Subregion Emission Factor',
        skiprows=[0, 1 ,2]
        )

    zip_sub_region = pd.concat(
        [pd.read_excel(e_grid_file, sheet_name=sheet) for sheet in
            ['Zip-subregion', 'Zip_multiple_Subregions']], axis=0,
        ignore_index=True
        )

    # Extracted from egrid summary tables for 2014 from
    # https://www.epa.gov/sites/production/files/2020-01/egrid2018_historical_files_since_1996.zip
    resource_mix_file = os.path.join(data_wd, 'egrid2014_resource_mix.csv')

    resource_mix = pd.read_csv(resource_mix_file)

    @classmethod
    def calc_egrid_emissions_resource_mix(cls):
        """
        Estimate county grid emissions factor (metric tons CO2e per MWh),
        resource mix, and losses as the mean of zip code egrid data.
        """

        fips_zips = pd.read_csv(cls.fips_to_zip_file)

        fips_zips.set_index('ZIP', inplace=True)

        subregions = pd.DataFrame(cls.zip_sub_region)

        subregions = pd.melt(subregions,
                             id_vars=['ZIP (character)', 'ZIP (numeric)',
                                      'state'], value_name='SUBRGN')

        subregions_ef_rm = pd.merge(subregions, cls.egrid_ef, on=['SUBRGN'],
                                    how='inner')

        subregions_ef_rm = pd.merge(subregions_ef_rm, cls.resource_mix,
                                    on=['SUBRGN'], how='left')

        new_cols = list(set(['SRCO2RTA','SRCH4RTA','SRN2ORTA']).union(
            resource_mix.columns[1:]
            ))

        subregions_ef_rm = subregions_ef_rm.groupby(
            ['ZIP (numeric)'], as_index=False
            )[new_cols].mean()

        subregions_ef_rm.rename(columns={'ZIP (numeric)':'ZIP'}, inplace=True)

        # Convert emissions fractors from lb/MWh to metric tons CO2e per MWh
        # (MTCO2e/MWh)
        subregions_ef_rm.loc[:, 'MTCO2e_per_MWh'] = \
            (subregions_ef_rm.SRCO2RTA + subregions_ef_rm.SRCH4RTA * 25 +
             subregions_ef_rm.SRN2ORTA * 298) * (0.453592 / 10**3)

        subregions_ef_rm = subregions_ef_rm.set_index('ZIP').join(fips_zips,
                                                            how='left')

        county_ef_rm = subregions_ef_rm.reset_index().groupby(
            'COUNTY_FIPS', as_index=False
            )['grid_losses', 'MTCO2e_per_MWh', 'Natural_gas', 'Coal', 'Oil',
              'Other_fossil','Solar', 'Biomass', 'Other', 'Hydro', 'Wind',
              'Nuclear','Geothermal'].mean()

        county_ef_rm['COUNTY_FIPS'] = county_ef_rm.COUNTY_FIPS.astype('int')

        county_ef_rm['MECS_FT'] = 'Net_electricity'

        return county_ef_rm
