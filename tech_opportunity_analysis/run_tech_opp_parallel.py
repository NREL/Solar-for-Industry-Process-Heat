
import numpy as np
import os
import sys
import time
import multiprocessing
import pickle
from tech_opp_calcs_parallel import tech_opportunity
from tech_opp_demand import demand_results
import tech_opp_h5
sys.path.append('../')
from heat_load_calculations.run_demand_8760 import demand_hourly_load

# Parameters for running tech opportunity
data_dir = 'c:/users/cmcmilla/desktop/'
tech_package = 'ptc_notes'
demand_filepath = 'ptc_process_energy.csv.gz'

rev_output_filepath = \
    'rev_output/ptc_notes/ptc_notes_sc0_t0_or0_d0_gen_2014.h5'

# Dictionary by tech package of all solar gen and process energy inputs
#  tech_opp_inputs = {
#   'dsg_lf': {'supply': 'dsg_lf/dsg_lf_sc0_t0_or0_d0_gen_2014.h5',
#              'demand': 'LF_process_energy.csv.gz'},
#    'swh': {'supply': 'swh/swh_sc0_t0_or0_d0_gen_2014.h5',
#            'demand': 'fpc_hw_process_energy.csv.gz'}
#    'pv_boiler': {'supply': 'pv/pv_sc0_t0_or0_d0_gen_2014.h5'
#                  'demand': 'eboiler_process_energy.csv.gz'},
#    'pv_resist': {'supply': 'pv/pv_sc0_t0_or0_d0_gen_2014.h5'
#                  'demand': ''}
#    'pv_whrhp': {'supply': 'pv/pv_sc0_t0_or0_d0_gen_2014.h5'
#                 'demand': ''},
#    'ptc_tes': {'supply': 'ptc_tes6hr_sc0_t0_or0_d0_gen_2014.h5',
#               'demand': 'ptc_process_energy.csv.gz'},
#    'ptc_notes': {'supply': 'ptc_notes_sc0_t0_or0_d0_gen_2014.h5',
#               'demand': 'ptc_process_energy.csv.gz'}}
#

# rev_output_filepath ='rev_output/{}/{}_sc0_t0_or0_d0_gen_2014.h5'.format(
#     tech_package, tech_package
#     )
sizing_month = 12

# Time stamp (in UTC) for h5 file.
time_stamp = time.strftime('%Y%m%d_%H%M', time.gmtime())

pickle_results = False

# Start timing calculations
start = time.time()

tech_opp_methods = tech_opportunity(tech_package,
                                    os.path.join(data_dir,
                                                 rev_output_filepath),
                                    sizing_month=sizing_month)

# Set to break out all fuels
tech_opp_methods.fuels_breakout = 'all'

# Instantiate process demand data (annual MMBtu) and methods
process_demand = demand_results(os.path.join(data_dir, demand_filepath))

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
            county, county_8760_ophours, fuels=tech_opp_methods.fuels_breakout
            )

        peak_demand = county_peak.xs(op_h)[0]

        # Calculate tech opportunity and associated land use
        tech_opp, tech_opp_land = \
            tech_opp_methods.tech_opp_county(county, county_8760_ophours,
                                             peak_demand)

        # Monthly fuel displaced (in MWh)
        fuels_displaced = process_demand.calc_fuel_displaced(
            county_fuel_fraction, tech_opp, county_8760_ophours
            )

        tech_opp = tech_opp.values

        # This is just the total county hourly load, summed across all
        # naics, emp sizes, and end uses
        county_sum = county_8760_ophours.sum(level=3).values

        if first:
            names = np.array((op_h,))
            county_total_load = county_sum
            county_tech_opp = tech_opp
            county_fuels_displaced = fuels_displaced
            county_tech_opp_land = tech_opp_land
            first = False

        else:
            names = np.vstack([names, np.array((op_h,))])
            county_total_load = np.hstack([county_total_load, county_sum])
            county_tech_opp = np.hstack([county_tech_opp, tech_opp])
            county_fuels_displaced = np.vstack([county_fuels_displaced,
                                                fuels_displaced])
            county_tech_opp_land = np.vstack([county_tech_opp_land,
                                              tech_opp_land])

    tech_opp_meta = tech_opp_methods.get_county_info(county, county_ind)

    return [tech_opp_meta, time_index, names, county_tech_opp,
            county_fuels_displaced, county_tech_opp_land, county_total_load]


if __name__ == "__main__":

    __spec__ = None

    counties = tuple(process_demand.demand_data.COUNTY_FIPS.unique())

    counties = check_county(counties)

    target_file_path = '{}_sizing_{}_{}.hdf5'.format(tech_package,
                                                     sizing_month, time_stamp)

    # Run calculations in parallel
    with multiprocessing.Pool(processes=6) as pool:
        results = pool.map(calc_county, counties[0:3])

    if pickle_results:
        pickle_file = open('tech_opps.pkl', 'wb')
        pickle.dump(results, pickle_file)
        pickle_file.close()

    tech_opp_h5.create_h5(target_file_path, results)

    # End timer
    end = (time.time()-start)
    print('---------')
    print("it took {} seconds to run.".format(np.round(end, 0)))
    print('---------')
