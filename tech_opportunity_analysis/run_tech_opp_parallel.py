import pandas as pd
import numpy as np
import os
import sys
import multiprocessing
import pickle
from tech_opp_calcs_parallel import tech_opportunity
from tech_opp_demand import demand_results
from format_rev_output import rev_postprocessing
from tech_opp_h5 import create_h5
sys.path.append('../')
from heat_load_calculations.run_demand_8760 import demand_hourly_load

# Parameters for running tech opportunity
data_dir = 'c:/users/cmcmilla/desktop/'
tech_package = 'swh'
demand_filepath= 'fpc_hw_process_energy.csv.gz'
rev_output_filepath ='rev_output/{}/{}_sc0_t0_or0_d0_gen_2014.h5'.format(
    tech_package, tech_package
    )
sizing_month = 12

# Time stamp (in UTC) for h5 file.
time_stamp = time.strftime('%Y%m%d-%H:%M', time.gmtime())

# pickle_results = False

tech_opp_methods = tech_opportunity(tech_package,
                                    os.path.join(data_dir, rev_output_filepath),
                                    sizing_month=sizing_month)

# Instantiate process demand data (annual MMBtu) and methods
process_demand = demand_results(os.path.join(data_dr, demand_filepath))

# Instantiate methods for creating hourly loads (in MW)
hourly_demand = demand_hourly_load(2014, process_demand.demand_data)

def check_county(process_counties):
    """
    Check that county is in demand and solar gen data sets.
    """

    checked = set(process_counties).intersection(
        process_demand.demand_data.COUNTY_FIPS.unique()
        ).intersection(
            tech_opp_methods.rev_output.county_info.index.values
        )

    return list(checked)

def calc_county(county):
    # Calculate hourly load for annual demand (in MW). Calculate December and
    # June demand (in MWh) for sizing generation
    print('County ', county)
    county_8760, county_peak = \
        hourly_demand.calculate_county_8760_and_peak(
            county, peak_month=tech_opp_methods.sizing_month, peak_MW=False
            )

    county_ind = process_demand.breakout_county_ind(county_8760)

    time_index = county_8760.index.get_level_values('index').unique()

    first = True
    for op_h in tech_opp_methods.op_hours:

        county_8760_ophours = county_8760.xs(op_h, level='op_hours')

        # Assign breakouts by fuel and other characteristics
        county_fuel_fraction = process_demand.county_load_fuel_fraction(
            county_8760_ophours
            )

        peak_demand = county_peak.xs(op_h)[0]

        # Calculate tech opportunity and associated land use
        tech_opp, tech_opp_land = \
            tech_opp_methods.tech_opp_county(county, county_8760_ophours,
                                             peak_demand)

        tech_opp_fuels = process_demand.breakout_fuels_tech_opp(
            county, county_fuel_fraction, tech_opp,
            tech_opp_methods.fuels_breakout
            )

        tech_opp = tech_opp.values

        if first:

            names = np.array((op_h,))

            county_tech_opp = tech_opp

            county_tech_opp_fuels = tech_opp_fuels

            county_tech_opp_land = tech_opp_land

            first = False

        else:

            names = np.vstack([names, np.array((op_h,))])

            county_tech_opp = np.hstack([county_tech_opp, tech_opp])

            county_tech_opp_fuels = np.vstack([county_tech_opp_fuels,
                                               tech_opp_fuels])

            county_tech_opp_land = np.vstack([county_tech_opp_land,
                                              tech_opp_land])

    tech_opp_meta = tech_opp_methods.get_county_info(county, county_ind)

    return [tech_opp_meta, time_index, names, county_tech_opp,
            county_tech_opp_fuels, county_tech_opp_land]


if __name__ == "__main__":

    __spec__ = None

    counties = tuple(process_demand.demand_data.COUNTY_FIPS.unique())

    counties = check_county(counties)

    # Start timing calculations
    start = time.time()

    # Run calculations in parallel
    with multiprocessing.Pool(processes=5) as pool:

        results = pool.map(calc_county, counties)

        # if pickle_results==True:
        #
        #     rpfile =  open('c:/users/cmcmilla/desktop/swh_test_20200619', 'wb')
        #     pickle.dump(results, rpfile)
        #     rpfile.close()
        #
        # else:
        #     pass

        create_h5(data_dir+'{}swh_sizing_{}_{}.h5'.format(
            tech_pacakge, sizing_month, timestamp
            ), results)

    # End timer
    end = (time.time()-start)
    print('---------')
    print("it took {} to run.".format(end))
    print('---------')
