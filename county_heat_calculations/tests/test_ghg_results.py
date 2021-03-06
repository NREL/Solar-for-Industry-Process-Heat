import pandas as pd
from ghg import Emissions
import pytest
import numpy as np
import os


ghg_calcs = Emissions(2014)

county_energy_data = pd.read_parquet(
    os.path.join('../results/', 'mfg_eu_temps_20191031_2322.parquet.gzip')
    )

emissions_results = ghg_calcs.calc_fuel_emissions(county_energy_data)

emissions_results = \
    emissions_results[(emissions_results.data_source=='mecs_ipf') &
                      (emissions_results.MECS_FT_byp!='Biomass')]

emissions_results.drop(['MTCO2e_per_MMBtu'], axis=1, inplace=True)

# Do a county aggregation
emissions_results = emissions_results.groupby(
    ['COUNTY_FIPS', 'MECS_FT', 'MECS_FT_byp'], as_index=False
    )[['MMBtu', 'MTCO2e_TOTAL']].sum()

emissions_results = pd.concat(
    [pd.merge(
        emissions_results[emissions_results.MECS_FT_byp.isnull()].set_index(
            ['MECS_FT']
            ),ghg_calcs.std_efs[
                ghg_calcs.std_efs.MECS_FT_byp.isnull()
                ].set_index('MECS_FT')['MTCO2e_per_MMBtu'],
        left_index=True, right_index=True, how='left'
        ).reset_index(),
    pd.merge(
        emissions_results[emissions_results.MECS_FT=='Other'].set_index(
            ['MECS_FT_byp']
            ),ghg_calcs.std_efs[
                ghg_calcs.std_efs.MECS_FT=='Other'
                ].set_index('MECS_FT_byp')['MTCO2e_per_MMBtu'],
        left_index=True, right_index=True, how='left'
        ).reset_index()], axis=0, ignore_index=True, sort=True
    )

emissions_results['calc_ef'] = emissions_results.MTCO2e_TOTAL.divide(
    emissions_results.MMBtu
    )

calc_ef = emissions_results.calc_ef.values
std_ef = emissions_results.MTCO2e_per_MMBtu.values
fts = ['%s-%s'%(x,y) for x,y in emissions_results[['MECS_FT', 'MECS_FT_byp']].values]

@pytest.mark.parametrize('calc_ef, std_ef, fts', zip(calc_ef, std_ef, fts))
def test_nonghgrp_calcs(calc_ef, std_ef, fts):
    """
    Emissions from nonghgrp data calculated using standard emission factors
    (EFs). Check calcualtions with assertion that EFs calculated from emissions
    results and energy data match
    """

    assert np.round(calc_ef, 5) == np.round(std_ef, 5)
