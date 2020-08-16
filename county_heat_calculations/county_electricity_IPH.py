
import pandas as pd
import Match_GHGRP_County_IPH as county_matching
import get_cbp
import Calculate_MfgEnergy_IPH

# Code from run_all_IPH.py. Used to calculate MECS intensities based on
# CBP establishment counts.
energy_ghgrp = pd.read_parquet(
        '../results/ghgrp_energy_20190801-2337.parquet',
        engine='pyarrow'
        )
cbp = get_cbp.CBP(2014)
tcm = county_matching.County_matching(2014)
ghgrp_matching = tcm.format_ghgrp(energy_ghgrp, cbp.cbp_matching)
cbp.cbp_matching = tcm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)
cbp_corrected = tcm.correct_cbp(cbp.cbp_matching)
tcmfg = Calculate_MfgEnergy_IPH.Manufacturing_energy(2014, energy_ghgrp)
tcmfg.update_naics(ghgrp_matching)
mecs_intensities = tcmfg.calc_intensities(cbp.cbp_matching)
mecs_intensities = \
    mecs_intensities[mecs_intensities.MECS_FT == 'Net_electricity']

# mecs_elec contains 'dummy' NAICS codes. Need to covert.
naics_mappings = pd.read_csv('./calculation_data/mecs_naics_2012.csv',
                             usecols=['MECS_NAICS_dummies', 'MECS_NAICS'])
mecs_intensities = pd.merge(mecs_intensities, naics_mappings,
                            on='MECS_NAICS_dummies', how='left')


# Calculate establishment counts from county-level energy data, as
# this represents processed CBP and GHGRP data.
# Calculate first set of GHGRP electricity use based on difference
# of establishment counts between matched CBP and corrected CBP data.
def merge_cbp_intensity_data(cbp_data, mecs_intensities):
    """Merge processed CBP data with MECS electricity intensities"""

    merged_data = pd.melt(cbp_data,
                          id_vars=['fips_matching', 'MECS_NAICS_dummies',
                                   'MECS_Region', 'fipstate', 'fipscty',
                                   'naics', 'COUNTY_FIPS', 'MECS_NAICS'],
                          value_vars=[x for x in tcmfg.empsize_dict.values()],
                          var_name=['Emp_Size'], value_name='est_count')

    merged_data = pd.merge(merged_data, mecs_intensities,
                           on=['MECS_Region', 'MECS_NAICS', 'Emp_Size'],
                           how='inner', suffixes=('', '_mecs'))

    merged_data.set_index(['COUNTY_FIPS', 'naics', 'Emp_Size'], inplace=True)

    return merged_data


# All establishments merged with regional electricity intensities
county_elec_all = merge_cbp_intensity_data(cbp.cbp_matching, mecs_intensities)

# All establishemts estimated with MECS data with regional electricity
# intensities
county_elec_mecs = merge_cbp_intensity_data(cbp_corrected, mecs_intensities)

# GHGRP establishments are the difference between original CBP and corrected
# CBP countes.
county_elec_ghgrp = county_elec_all.copy(deep=True)
county_elec_ghgrp.est_count.update(
    county_elec_all.est_count.subtract(county_elec_mecs.est_count)
    )

# Calculate electricity for MECS-associated establishments
county_elec_mecs.reset_index(inplace=True)
county_elec_mecs.loc[:, 'MMBtu'] = county_elec_mecs.est_count.multiply(
    county_elec_mecs.intensity
    )*10**6

# Calculate electricity for GHGRP establishemts
county_elec_ghgrp.reset_index(inplace=True)
county_elec_ghgrp.loc[:, 'MMBtu'] = county_elec_ghgrp.est_count.multiply(
    county_elec_ghgrp.intensity
    )*10**6
county_elec_ghgrp.loc[:, 'Emp_Size'] = 'ghgrp'
# Use an average electricity intensity from county_elec_ghgrp on these
# remaining establishemtss.
remaining_ghgrp_intensity = \
    county_elec_ghgrp.groupby(['MECS_Region', 'MECS_NAICS',
                               'Emp_Size']).intensity.mean()
county_elec_ghgrp = county_elec_ghgrp.groupby(
    ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS', 'Emp_Size'],
    as_index=False)[['est_count', 'MMBtu']].sum()

# Read in county data set for final establishment counts.
# There are GHGRP facilities that were not matched by county FIPS and NAICS
# code and they need to be accounted for.
# The employment size class of GHGRP facilities is not known.
county_heat_est = pd.read_parquet(
    '../results/mfg_energy_total_20200728-0804.parquet.gzip',
    columns=['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS', 'Emp_Size',
             'est_count'])
county_heat_est.drop_duplicates(['COUNTY_FIPS', 'naics', 'Emp_Size'],
                                inplace=True)
county_heat_est = county_heat_est[county_heat_est.Emp_Size == 'ghgrp']
county_heat_est.set_index(['COUNTY_FIPS', 'naics', 'Emp_Size'], inplace=True)
county_heat_est.sort_index(inplace=True)
county_elec_ghgrp.set_index(['COUNTY_FIPS', 'naics', 'Emp_Size'], inplace=True)
county_heat_est.est_count.update(
    county_heat_est.est_count.subtract(county_elec_ghgrp.est_count)
    )

county_heat_est.reset_index(inplace=True)
county_heat_est.set_index(['MECS_Region', 'MECS_NAICS', 'Emp_Size'],
                          inplace=True)
county_heat_est = county_heat_est.join(remaining_ghgrp_intensity)
county_heat_est.reset_index(inplace=True)
county_heat_est.loc[:, 'MMBtu'] = county_heat_est.est_count.multiply(
    county_heat_est.intensity
    )*10**6

# Concatenate three sets of electricity estimates.
county_elec = pd.concat([county_elec_mecs, county_elec_ghgrp, county_heat_est],
                        axis=0, ignore_index=True, sort=False)

county_elec = county_elec[county_elec.est_count > 0]

county_elec.to_csv('../results/county_elec_estimates.csv.gzip', index=False,
                   compression='gzip')
