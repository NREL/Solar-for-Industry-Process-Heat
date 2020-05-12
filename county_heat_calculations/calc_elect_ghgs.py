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

class GHGs(object):
    """
    Class containing methods used to calculate electricity emission factors
    (MTCO2e/MMBtu) by county from EPA eGRID data.
    """

    data_wd = '../../Industry-Energy-Tool/data_for_calculations/ghgs/'

    emission_factor_file = os.path.join(data_wd,'Emission factors.xlsx')

    fips_to_zip_file = os.path.join(data_wd,'COUNTY_ZIP_032014.csv')

    e_grid_file = \
        os.path.join(data_wd,'power_profiler_zipcode_tool_2014_v7.1_1.xlsx')

    egrid_ef = pd.read_excel(
        e_grid_file, sheet_name='eGRID Subregion Emission Factor',
        skiprows=[0, 1 ,2]
        )

    zip_sub_region = pd.read_excel(e_grid_file, sheet_name='Zip-subregion')

    # Should be downloaded as https://data.nrel.gov/files/97/County_industry_energy_use.gz
    # from https://dx.doi.org/10.7799/1481899.
    #county_energy_file = # See comments above

    fuel_efs = pd.read_excel(
        emission_factor_file, sheet_name='emission_factors', index_col=[0]
        )

    resource_mix_file = os.path.join(data_wd, 'egrid2014_resource_mix.csv')

    resource_mix = pd.read_csv(resource_mix_file)

    @classmethod
    def calc_egrid_emissions_resource_mix(cls):

        fips_zips = pd.read_csv(cls.fips_to_zip_file)

        fips_zips.set_index('ZIP', inplace=True)

        subregions = pd.DataFrame(cls.zip_sub_region)

        subregions = pd.melt(subregions,
                             id_vars=['ZIP (character)', 'ZIP (numeric)',
                                      'state'], value_name='SUBRGN')

        subregions_ef_rm = pd.merge(subregions, cls.egrid_ef, on=['SUBRGN'],
                                    how='left')

        subregions_ef_rm = pd.merge(subregions_ef_rm, cls.resource_mix,
                                    on=['SUBRGN'], how='left')

        new_cols = set(['SRCO2RTA','SRCH4RTA','SRN2ORTA']).union(
            cls.resource_mix.columns[1:]
            )

        subregions_ef_rm = subregions_ef_rm.groupby(
            ['ZIP (numeric)'], as_index=False
            )[new_cols].mean()

        subregions_ef_rm.rename(columns={'ZIP (numeric)':'ZIP'}, inplace=True)

        subregions_ef_rm.loc[:, 'MTCO2e_per_MMBtu'] = \
            (subregions_ef_rm.SRCO2RTA + subregions_ef_rm.SRCH4RTA * 25 +
             subregions_ef_rm.SRN2ORTA * 298) * (0.453592 / 3.412 / 10**3)

        subregions_ef_rm = subregions_ef_rm.set_index('ZIP').join(fips_zips,
                                                            how='left')

        county_ef_rm = subregions_ef_rm.reset_index().groupby(
            'COUNTY_FIPS', as_index=False
            ).MTCO2e_per_MMBtu.mean()

        county_ef_rm['COUNTY_FIPS'] = county_ef_rm.COUNTY_FIPS.astype('int')

        county_ef_rm['MECS_FT'] = 'Net_electricity'

        return county_ef_rm
