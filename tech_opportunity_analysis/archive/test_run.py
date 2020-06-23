import pytest
import tech_opp_calcs_parallel

tp = tech_opp_calcs_parallel.tech_opportunity(
    'swh', 'c:/users/cmcmilla/desktop/fpc_hw_process_energy.csv.gz',
    'c:/Users/cmcmilla/Desktop/rev_output/swh/swh_sc0_t0_or0_d0_gen_2014.h5'
    )

## need to check that tp.demand == tp.demand_hourly

def test_county(county):

    test = tp.calc_tech_opp(county)

    return test

if __name__ == '__main__':

    test = test_county(1001)
