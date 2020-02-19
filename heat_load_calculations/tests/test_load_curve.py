import pytest
import make_load_curve
import numpy as np
import pandas as pd

# Create all combinations of naics and emp_size
naics_emp = pd.read_parquet(
    'c:/users/cmcmilla/solar-for-industry-process-heat/results/mfg_eu_temps_20191031_2322.parquet.gzip',
    columns=['naics', 'Emp_Size']
    )

# naics_emp = naics_emp.drop_duplicates().values

naics_emp = [(321219, 'n100_249')]
@pytest.mark.parametrize("naics, emp_size", naics_emp)
def test_make_load_curve(naics, emp_size):

    annual_mmbtu = 100
    # naics_og = 331110
    #
    # emp_size = 'n1_4'

    hours = 'qpc'

    energy = 'heat'

    lc = make_load_curve.load_curve()

    load_shape = lc.calc_load_shape(
        naics, emp_size, enduse_turndown={'boiler': 4}, hours=hours,
        energy=energy
        )

    load_8760 = lc.calc_annual_load(annual_mmbtu, load_shape)

    # Check that the sum of hourly demand equals the annual demand
    assert all(
        [np.around(x, 1) == annual_mmbtu for x in load_8760[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].sum()]
        )
