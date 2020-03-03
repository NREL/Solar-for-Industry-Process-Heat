import pytest
import make_load_curve
import numpy as np
import pandas as pd
import logging



# Create all combinations of naics and emp_size
naics_emp = pd.read_parquet(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/mfg_eu_temps_20191031_2322.parquet.gzip',
    columns=['naics', 'Emp_Size']
    )

naics_emp = naics_emp.drop_duplicates().values

@pytest.mark.parametrize("naics, emp_size", naics_emp)
def test_make_load_curve(naics, emp_size):
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler('test_load_curve.log')
    logger.addHandler(handler)

    annual_mmbtu = 100

    hours = 'qpc'

    energy = 'heat'

    lc = make_load_curve.load_curve()

    load_shape = lc.calc_load_shape(
        naics, emp_size, enduse_turndown={'boiler': 4}, hours=hours,
        energy=energy
        )

    load_8760 = lc.calc_annual_load(annual_mmbtu, load_shape)

    # Check that the sum of hourly demand equals the annual demand
    logger.error('No assert! Bad assert!', exc_info=True)
    assert all(
        [np.around(x, 1) == annual_mmbtu for x in load_8760[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].sum()]
        )
