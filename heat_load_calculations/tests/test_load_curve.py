import pytest
import make_load_curve

def test_make_load_curve():

    annual_mmbtu = 100

    naics_og = 331110

    emp_size = 'n250_499'

    hours = 'qpc'

    energy = 'heat'

    lc = make_load_curve.load_curve()

    load_factor, min_peak_loads = \
        lc.load_data.select_load_data(naics_og, emp_size)

    load_shape = lc.calc_load_shape(
        naics_og, emp_size, enduse_turndown={'boiler': 4}, hours=hours,
        energy=energy
        )

    print('load shape:\n', load_shape)

    load_8760 = lc.calc_annual_load(annual_mmbtu, load_shape)

    # Check that the monthly sum of load shape equals the monthly load factor
    assert all(load_shape.sum(level=0) == load_factor)


    # Check that the sum of hourly demand equals the annual demand
    assert all(
        load_8760[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].sum() == annual_mmbtu
        )
