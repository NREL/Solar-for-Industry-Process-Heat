#!/usr/bin/env python

# command line args
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--tech_package', required=True, help='Name of solar tech package: swh, dsg_lf, ptc_notes, ptc_tes, pv_ac, or pv_dc')
parser.add_argument('--counties', required=False, help='List of county FIPS to evaluate. Default value is all')
parser.add_argument('--demand_path', required=True, help='Path to demand file (a gzip csv)')
parser.add_argument('--supply_path', required=True, help='Path to supply file (an h5)')
# parser.add_argument('--output_path', required=True, help='Path and filename to save results (h5 file)')

args = parser.parse_args()

import os
# from pathos.multiprocessing import ProcessPool
from tech_opp_calcs import tech_opportunity
import numpy as np
import datetime
import time
import sys
import pickle


tech_package = args.tech_package
counties = args.counties.split(',') if args.counties else []
demand_path = args.demand_path
supply_path = args.supply_path
# output_path = args.output_path


# if __name__=='__main__':

# freeze_support()

# pool = ProcessPool(nodes=3)

tech_opp = tech_opportunity(tech_package, demand_path, supply_path)

if counties == []:

    process_counties = list(tech_opp.demand.demand_data.COUNTY_FIPS.unique())

else:

    process_counties = [int(x) for x in counties]

# results = pool.map(tp.tech_opp_county, process_counties)

final_results = []

start = time.time()
for county in process_counties:

    final_results.append(tech_opp.tech_opp_county(county))

end = (time.time()-start)
print('---------')
print("it took {}".format(end))
print('---------')

# Pickle time
with open('tech_opp_results', 'wb') as fp:
    pickle.dump(final_results, fp)


# def append_results(tech_opp_dict, results):
#     """
#     Method to append dictionary of technical opportunity results.
#     """
#
#     # Check if empty
#     if not tech_opp_dict:
#
#         return results
#
#     def key_check_append(tech_opp_dict_k, results_k):
#
#         try:
#
#             tech_opp_dict_k.keys()
#
#         except AttributeError:
#
#             tech_opp_dict_k = np.hstack(
#                 [tech_opp_dict_k, results_k]
#                 )
#
#             return tech_opp_dict_k
#
#         else:
#
#             return 'skip'
#
#     for k1 in tech_opp_dict.keys():
#
#         kcheck = key_check_append(tech_opp_dict[k1], results[k1])
#
#         if kcheck=='skip':
#
#             for k2 in tech_opp_dict[k1].keys():
#
#                 kcheck2 = key_check_append(tech_opp_dict[k1][k2],
#                                            results[k1][k2])
#
#                 if kcheck2=='skip':
#
#                     for k3 in tech_opp_dict[k1][k2].keys():
#
#                         kcheck3 = key_check_append(
#                             tech_opp_dict[k1][k2][k3],
#                             results[k1][k2][k3]
#                             )
#
#                         if kcheck3=='skip':
#
#                             for k4 in tech_opp_dict[k1][k2][k3].keys():
#
#                                 kcheck4 = key_check_append(
#                                     tech_opp_dict[k1][k2][k3][k4],
#                                     results[k1][k2][k3][k4]
#                                     )
#
#                                 if kcheck4=='skip':
#
#                                     kcheck5 = key_check_append(
#                                         tech_opp_dict[k1][k2][k3][k4][k5],
#                                         results[k1][k2][k3][k4][k5]
#                                         )
#                                 else:
#
#                                     tech_opp_dict[k1][k2][k3][k4] = kcheck4
#
#                         else:
#
#                             tech_opp_dict[k1][k2][k3] = kcheck3
#
#                 else:
#
#                     tech_opp_dict[k1][k2] = kcheck2
#
#         else:
#
#             tech_opp_dict[k1] = kcheck
#
#     return tech_opp_dict
