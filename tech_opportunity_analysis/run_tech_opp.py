#!/usr/bin/env python

# command line args
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--tech_package', required=True, help='Name of solar tech package: swh, dsg_lf, ptc_notes, ptc_tes, pv_ac, or pv_dc')
parser.add_argument('--counties', required=True, help='List of county FIPS to evaluate. Default value is all')
parser.add_argument('--demand_path', required=True, help='Path to demand file (a gzip csv)')
parser.add_argument('--supply_path', required=True, help='Path to supply file (an h5)')
parser.add_argument('--output_path', required=True, help='Path and filename to save results (h5 file)')

args = parser.parse_args()

import os
import multiprocessing
import tech_opp_calcs
import tech_opp_h5
import numpy as np
import datetime


tech_package = args.tech_package
counties = args.counties
demand_path = args.demand_path
supply_path = args.supply_path
output_path = args.output_path

def append_results(tech_opp_dict, results):
    """
    Method to append dictionary of technical opportunity results.
    """

    # Check if empty
    if not tech_opp_dict:

        return results

    def key_check_append(tech_opp_dict_k, results_k):

        try:

            tech_opp_dict_k.keys()

        except AttributeError:

            tech_opp_dict_k = np.hstack(
                [tech_opp_dict_k, results_k]
                )

            return tech_opp_dict_k

        else:

            return 'skip'

    for k1 in tech_opp_dict.keys():

        kcheck = key_check_append(tech_opp_dict[k1], results[k1])

        if kcheck=='skip':

            for k2 in tech_opp_dict[k1].keys():

                kcheck2 = key_check_append(tech_opp_dict[k1][k2],
                                           results[k1][k2])

                if kcheck2=='skip':

                    for k3 in tech_opp_dict[k1][k2].keys():

                        kcheck3 = key_check_append(
                            tech_opp_dict[k1][k2][k3],
                            results[k1][k2][k3]
                            )

                        if kcheck3=='skip':

                            for k4 in tech_opp_dict[k1][k2][k3].keys():

                                kcheck4 = key_check_append(
                                    tech_opp_dict[k1][k2][k3][k4],
                                    results[k1][k2][k3][k4]
                                    )

                                if kcheck4=='skip':

                                    kcheck5 = key_check_append(
                                        tech_opp_dict[k1][k2][k3][k4][k5],
                                        results[k1][k2][k3][k4][k5]
                                        )
                                else:

                                    tech_opp_dict[k1][k2][k3][k4] = kcheck4

                        else:

                            tech_opp_dict[k1][k2][k3] = kcheck3

                else:

                    tech_opp_dict[k1][k2] = kcheck2

        else:

            tech_opp_dict[k1] = kcheck

    return tech_opp_dict

def tech_opp_parallel(counties):

    tech_opp_results = {}

    if counties == 'all':

        process_counties = tp.demand.demand_data.COUNTY_FIPS.unique()

    else:

        process_counties = counties

    for county in process_counties:

        print(county)

        tech_opp_county = tp.tech_opp_county(county)

        tech_opp_results = append_results(tech_opp_results, tech_opp_county)

    # with multiprocessing.Pool() as pool:
    #
    #     tech_opp_results = append_results(
    #         pool.starmap(tp.tech_opp_county, process_counties), tech_opp_results
    #         )

    # for county in process_counties:
    #
    #     print(county)
    #
    #     to_process = multiprocessing.Process(target=tp.tech_opp_county,
    #                                          args=(county,))
    #
    #     to_process.start()
    #
    #     tech_opp_results = append_results(tech_opp_results, to_process)
    #
    #     to_process.join()

    # for to_process in tech_opp_results:
    #
    #     to_process.join()

    return tech_opp_results


tp = tech_opp_calcs.tech_opportunity(tech_package, demand_path, supply_path)

tech_opp_results = tech_opp_parallel(counties)

tech_opp_h5.create_h5('swh_tech_opp.h5', tech_opp_results)

# tech_opp_h5.create_h5(output_path, tech_opp_results)



            #
            # print('complete')
            #
            # ind_forecasts.to_csv('ind_forecasts.csv', index=False)
# if __name__ == 'main':
#
#     tp = tech_opp_calcs(tech_package, demand_path, supply_path)
#
#     tech_opp_results = {}
#
#     if counties == 'all':
#
#         process_counties = tp.demand.demand_data.COUNTY_FIPS.unique()
#
#     else:
#
#         process_counties = counties
#
#     for county in process_counties:
#
#
#         to_process = multiprocessing.Process(target=tp.tech_opp_county,
#                                              args=(county,))
#
#         tech_opp_results = append_results(tech_opp_results, tech_opp)
#
#         to_proces.start()
#
#         to_proces.join()
#
#     tech_opp_h5.create_h5(output_path, tech_opp_results)
