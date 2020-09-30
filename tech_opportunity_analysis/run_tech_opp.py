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


tech_opp = tech_opportunity(tech_package, demand_path, supply_path)

if counties == []:

    process_counties = list(tech_opp.demand.demand_data.COUNTY_FIPS.unique())

else:

    process_counties = [int(x) for x in counties]

def check_county(process_counties):
    """
    Check that county is in demand and solar gen data sets.
    """

    checked = set(process_counties).intersection(
        tech_opp.demand.demand_data.COUNTY_FIPS.unique()
        ).intersection(
            tech_opp.rev_output.county_info.index.values
        )

    return list(checked)

process_counties = check_county(process_counties)
# results = pool.map(tp.tech_opp_county, process_counties)

final_results = []

start = time.time()
n=1
for county in process_counties:

    # 46113 shouldn't be appearing in process
    if county == 46113:

        print('There\'s that {} again'.format(county))

        continue

    else:

        print('Now running county {}, {} of {} counties'.format(
            county, n, len(process_counties)
            ))

        final_results.append(tech_opp.tech_opp_county(county))

    n+=1

end = (time.time()-start)
print('---------')
print("it took {}".format(end))
print('---------')

# Pickle time
with open('tech_opp_results', 'wb') as fp:
    pickle.dump(final_results, fp)
