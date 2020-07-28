# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 20:58:33 2019

@author: cmcmilla
"""
import Match_GHGRP_County_IPH as county_matching
import get_cbp
import pandas as pd
import Calculate_MfgEnergy_IPH
import mecs_ipf_IPH as ipf
import MECS_IPF_seed_format_IPH as ipf_seed
import mecs_table5_2_formatting
import datetime as dt
import dask.dataframe as dd
import os

today = dt.datetime.now().strftime('%Y%m%d-%H%M')

energy_ghgrp = pd.read_parquet(
        '../results/ghgrp_energy_20190801-2337.parquet',
        engine='pyarrow'
        )
#%%
cbp = get_cbp.CBP(2014)

tcm = county_matching.County_matching(2014)

ghgrp_matching = tcm.format_ghgrp(energy_ghgrp, cbp.cbp_matching)

cbp.cbp_matching = tcm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)

cbp_corrected = tcm.correct_cbp(cbp.cbp_matching)

# Instantiate class for a single year
tcmfg = Calculate_MfgEnergy_IPH.Manufacturing_energy(2014, energy_ghgrp)

# update NAICS codes for energy_ghgrp based on ghgrp_matching
tcmfg.update_naics(ghgrp_matching)

ghgrp_mecstotals = tcmfg.GHGRP_Totals_byMECS()

seed_methods = ipf_seed.IPF_seed(year=2014)

seed_df = seed_methods.create_seed(cbp.cbp_matching)

ipf_methods = ipf.IPF(2014, table3_2=seed_methods.table3_2,
                   table3_3=seed_methods.table3_3)

# Run IPF. Saves resulting energy values as csv
ipf_methods.mecs_ipf(seed_df)

mecs_intensities = tcmfg.calc_intensities(cbp.cbp_matching)
#%%
# Calculates non-ghgrp combustion energy use and combines with
# ghgrp energy use. Distinguishes between data sources with 'data_source'
# column.
mfg_energy = tcmfg.combfuel_calc(cbp_corrected, mecs_intensities)

mfg_energy.to_parquet('../results/mfg_energy_total_'+today+'.parquet.gzip',
                      engine='pyarrow', compression='gzip')

enduse_methods = mecs_table5_2_formatting.table5_2(2014)

enduse_fraction = enduse_methods.calculate_eu_share()

# Enduse breakdown without temperatures.
# This returns a dask dataframe.
mfg_energy_enduse = tcmfg.calc_enduse(enduse_fraction, mfg_energy,
                                      temps=False)
mfg_energy_enduse.to_parquet('../results/mfg_eu_'+today+'.parquet.gzip',
                             index=True,engine='pyarrow',compression='gzip')
# Save as parquet
# os.mkdir('../results/mfg_eu_'+today)


# dd.to_parquet(
#         mfg_energy_enduse,
#         '../results/mfg_eu_'+today,
#         write_index=True, engine='pyarrow', compression='gzip'
#         )

# Enduse breakdown with temperatures; Returns only process heating end uses
# with defined temperatures.
# This returns a Pandas dataframe
mfg_energy_enduse_temps = tcmfg.calc_enduse(enduse_fraction, mfg_energy,
                                            temps=True)

# Save as parquet
mfg_energy_enduse_temps.to_parquet(
        '../results/mfg_eu_temps_'+dt.datetime.now().strftime('%Y%m%d_%H%M')+\
        '.parquet.gzip', engine='pyarrow', compression='gzip'
        )
