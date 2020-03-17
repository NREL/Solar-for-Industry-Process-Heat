
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

county_energy = county_energy.groupby(['COUNTY_FIPS', 'End_use'],
                                      as_index=False).MMBtu.sum()

county_energy.set_index('COUNTY_FIPS', inplace=True)

county_files = os.listdir(
  'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
  'national_loads_8760'
  )

@pytest.mark.parametrize("county_file", county_files)
def test_county_total(county_file):
  # Something wrong with this logging. Not working.
  logger = logging.getLogger(__name__)
  handler = logging.FileHandler('test_national_8760.log', mode='w')
  logger.addHandler(handler)
  logger.setLevel(logging.DEBUG)

  file_dir = 'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
    'national_loads_8760'

  county = int(re.search(r'(?<=_)\w+', county_file)[0])

  counties = [48201]

  if county in counties:

    # read in parquet of county load
    county_8760 = pd.read_parquet(os.path.join(file_dir, county_file),
                                  engine='pyarrow')

    county_8760 = np.around(
      county_8760.groupby(['op_hours', 'End_use']).load_MMBtu_per_hour.sum(), 1
      )

    county_sum = np.tile(
      np.around(county_energy.xs(county)['MMBtu'].values, 1), 3
      )

    logger.error('No assert! Bad assert!''\n''County: %s''\n''County_sum: %s'\
                 '\n''County_8760_sum: %s' % (county, county_sum, county_8760),
                 exc_info=True)

    assert all(county_sum == county_8760)

  else:

    return

# Counties failing test:
# 15001, 19141, 21059, 39019, 4013, 45091, 8085, 8089
