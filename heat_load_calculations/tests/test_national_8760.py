
import pytest
import pandas as pd
import os
import logging
import numpy as np
import re

# Import and group sum by county
county_energy = pd.read_parquet(
  'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
  'mfg_eu_temps_20191031_2322.parquet.gzip', engine='pyarrow'
  )

county_energy = county_energy.groupby('COUNTY_FIPS', as_index=False).MMBtu.sum()

county_energy.set_index('COUNTY_FIPS', inplace=True)

county_files = os.listdir(
  'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
  'national_loads_8760'
  )

@pytest.mark.parametrize("county_file", county_files)
def test_county_total(county_file):
  logger = logging.getLogger(__name__)
  handler = logging.FileHandler('test_national_8760.log')
  logger.addHandler(handler)

  file_dir = 'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
    'national_loads_8760'

  county = int(re.search(r'(?<=_)\w+', county_file)[0])

  # read in parquet of county load
  county_8760 = pd.read_parquet(os.path.join(file_dir, county_file),
                                engine='pyarrow')

  county_8760 = np.around(
    county_8760.groupby('op_hours').load_MMBtu_per_hour.sum(), 3
    )

  county_sum = np.around(county_energy.xs(county).values[0],3)

  logger.info("County:", county, '\n', 'County_sum:', county_sum,
              '\n', 'County_8760_sum:', county_8760)
  logger.error('No assert! Bad assert!', exc_info=True)
  assert all(county_sum == county_8760)
