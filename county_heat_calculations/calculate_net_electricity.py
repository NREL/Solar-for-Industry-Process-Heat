import Match_GHGRP_County_IPH as county_matching
import get_cbp
import pandas as pd
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d-%H%M')
# Import GHGRP energy data
energy_ghgrp = pd.read_parquet('../results/ghgrp_energy_20200826-1725.parquet',
                               engine='pyarrow')
cbp = get_cbp.CBP(2014)  # Import county business patterns data
tcm = county_matching.County_matching(2014)  # Instantiate matching methods
# Match GHGRP facilities to their county
ghgrp_matching = tcm.format_ghgrp(energy_ghgrp, cbp.cbp_matching)
cbp.cbp_matching = tcm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)

# Adjust the CBP establishment counts based on GHGRP facilities
cbp_corrected = tcm.correct_cbp(cbp.cbp_matching)

# Import results of IPF algorithm applied to 2014 MECS.
ipf_results_formatted = pd.read_csv(
    './calculation_data/mecs_2014_ipf_results_naics_employment.csv',
    index_col=0
    )


def calculate_net_electricity(cbp_matching, ipf_results_formatted):
    """
    Estimates net electricity by county, industry, and employment size class
    for all establishments. Applies net electricity intensities calculated
    from MECS to all establishments, including GHGRP facilities, unlike
    combustion emissions estimates.
    According to [EIA](https://www.eia.gov/consumption/manufacturing/terms.php#n):
    >*Net electricity* is estimated for each manufacturing establishment as the
    sum of purchased electricity, transfers in, and generation from
    noncombustible renewable resources minus the quantities of electricity
    sold and transferred offsite. Thus net electricity excludes the quantities
    of electricity generated or cogenerated onsite from combustible energy
    sources.

    Parameters
    ----------
    cbp_matching : pandas DataFrame
        County Business Pattern (CBP) data that have been matched to
        GHGRP facilities and undergone various reformatting in
        `Match_GHGRP_County_IPH.py`

    ipf_results_formatted : pandas Dataframe
        Results of iterative proportional fitting (IPF) algorithm applied
        to 2014 MECS data via `mecs_ipf_IPH.py`.

    Returns
    -------
    net_electricity : pandas DataFrame
        DataFrame of estimated net electricity values


    """
    empsize_dict = {'Under 50': 'n1_49', '50-99': 'n50_99',
                    '100-249': 'n100_249', '250-499': 'n250_499',
                    '500-999': 'n500_999', '1000 and Over': 'n1000'}

    net_electricity = pd.melt(
        ipf_results_formatted[ipf_results_formatted.MECS_FT=='Net_electricity'],
        id_vars=['MECS_Region', 'Emp_Size', 'MECS_FT'],
        var_name=['MECS_NAICS_dummies'], value_name='energy'
        )

    net_electricity['MECS_NAICS_dummies'] =\
        net_electricity.MECS_NAICS_dummies.astype('int')

    net_electricity.set_index(['MECS_Region', 'MECS_NAICS_dummies',
                               'Emp_Size'], inplace=True)

    cbp_grpd = cbp_matching.groupby(['MECS_Region', 'fips_matching', 'naics',
                                     'MECS_NAICS_dummies'],
                                    as_index=False).sum()

    cbp_grpd = pd.melt(cbp_grpd,
                       id_vars=['MECS_Region', 'fips_matching', 'naics',
                                'MECS_NAICS_dummies'],
                       value_vars=[x for x in empsize_dict.values()],
                       var_name=['Emp_Size'], value_name='est_count')

    cbp_grpd.set_index(['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size',
                        'naics', 'fips_matching'], inplace=True)

    cbp_grpd = cbp_grpd.where(cbp_grpd.est_count > 0).dropna()

    net_electricity = cbp_grpd.join(net_electricity)

    # Calculate net electricity by county by apportioning based on
    # establishment counts.
    elect_est_count = net_electricity.est_count.divide(
        net_electricity.est_count.sum(level=[0, 1, 2])
        )
    net_elect_MMBtu = pd.DataFrame(elect_est_count.multiply(
        net_electricity.energy
        )*10**6)  # convert from TBtu to MMBtu

    # net_electricity.energy.update(
    #     net_electricity.est_count.divide(
    #         net_electricity.est_count.sum(level=[0, 1, 2])
    #         ).multiply(net_electricity.energy)*10**6  #convert from TBtu to MMBtu
    #     )

    net_elect_MMBtu.reset_index(inplace=True)
    net_elect_MMBtu.rename(columns={0: 'MMBtu',
                                    'fips_matching': 'COUNTY_FIPS'},
                           inplace=True)
    net_elect_MMBtu['MECS_FT'] = 'Net_electricity'

    return net_elect_MMBtu


net_elect_MMBtu = calculate_net_electricity(cbp.cbp_matching,
                                            ipf_results_formatted)
net_elect_MMBtu.to_csv(
    '../results/county_elec_estimates_{}.csv.gzip'.format(today),
    index=False, compression='gzip'
    )
