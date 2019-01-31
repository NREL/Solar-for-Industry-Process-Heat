# -*- coding: utf-8 -*-
"""
Created on Fri Jan 13 13:45:19 2017
@author: ookie
Modified on Thur Feb 9 16:09:00 2017 by colin
"""

import pandas as pd
import numpy as np
import itertools as it
import MECS2014_format
import get_cbp

#import CBP data for 2014
cbp = get_cbp.CBP(2014)

MECS2014_format.Format_seed()

seed = MECS2014_format.Format_seed().import_seed()

seed = MECS2014_format.Format_seed().seed_correct_cbp(seed, cbp.cbp)

def IPF(naics_df, emply_df, seed_df):
    """
    Method for running iterative proportional fitting (IPF) on
    formatted MECS tables 3.2 and 3.3.
    """
    colDict = {'regions': ['Northeast', 'Midwest', 'South', 'West'],
               'energy': ['Net_electricity', 'Residual_fuel_oil', 'Diesel',
                          'Natural_gas', 'LPG_NGL', 'Coal',
                          'Coke_and_breeze','Other'],
               'employment': ['Under 50', '50-99','100-249', '250-499',
                              '500-999', '1000 and Over'],
               'value': ['Under 20', '20-49', '50-99', '100-249', '250-499',
                         '500 and Over']}

    def combine(columns):
        """
        Takes values in the dictionary above and creates a list of all
        combinations
        """
        labels = [colDict[x] for x in columns]
        labels = list(it.product(*labels))
        output = []
        for i,label in enumerate(labels):
            output.append('_'.join(label))
        return output

    #create column headings that have combinations of regions and energy carriers
    headings = combine(['regions','energy'])

    headings

    #two-dimensional iterative proportional fitting algorithm
    def ipf2D_calc(seed, col, row):

        #col matrix should have dimensions of (m,1)
        #row matrix should have dimensions of (1,n)
        #seed matrix should have dimensions of (m,n)
        col_dim = col.shape[0]
        row_dim = row.shape[1]

        for n in range(3000): #set maximumn number of iterations
            error = 0.0
            #faster 'pythonic(?)' version
            sub = seed.sum(axis=1,keepdims=True)
            sub = col / sub
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:,np.newaxis],row_dim,axis=1)
            seed = seed*sub
            diff = (seed.sum(axis=1,keepdims=True)-col)
            diff = diff*diff
            error += diff.sum()
            sub = seed.sum(axis=0,keepdims=True)
            sub = row / sub
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:,np.newaxis],col_dim,axis=1)
            sub = sub.transpose()
            seed = seed*sub
            diff = (seed.sum(axis=0,keepdims=True)-row)
            diff = diff*diff
            diff = diff.sum()
            error = np.sqrt(error)
            if error < 1e-15: break
        #report error if max iterations reached
        if error > 1e-13: print("Max Iterations ", error)
        return seed

    seed_shop = seed_df
    seed_shop = seed_shop.T
    seed_shop_dict = {}

    # Iterate through all of the fuel types
    for r in range(0, 8):

        counter = 6 * r

        seed_shop_dict['Northeast'] = seed_shop.iloc[
            :, (0 + counter):(6 + counter)
            ]
        seed_shop_dict['Midwest'] = seed_shop.iloc[
            :, (6 + counter):(12 + counter)
            ]
        seed_shop_dict['South'] = seed_shop.iloc[
            :, (12 + counter):(18 + counter)
            ]
        seed_shop_dict['West'] = seed_shop.iloc[
            :, (18 + counter):(24 + counter)
            ]

        first = True

        for heading in headings:
            print(heading)
            col = naics_df[heading].as_matrix(columns=None)
            row = emply_df[heading].as_matrix(columns=None)

            col = np.array([col])
            row = np.array([row])
            col = np.transpose(col)

            #lacking microdata sample, set the inital guess as all ones, except
            #instances where CBP fac count = 0 & MECS energy != 0
            seed = np.array(seed_shop_dict[heading.split("_")[0]])
            #seed = np.ones((col.shape[0],row.shape[1]))

            col = col.astype(float)
            row = row.astype(float)

            if first: naics_emply = ipf2D_calc(seed, col, row)
            else: naics_emply = np.hstack(
                (naics_emply, ipf2D_calc(seed, col, row))
                )
            first = False

        headings = np.array(combine(['regions','energy','employment']))

        naics_emply = np.vstack((headings, naics_emply))

    #Need to add column of MECS_NAICS_dummies
    filename = 'naics_employment.csv'
    np.savetxt(path + filename, naics_emply, fmt='%s', delimiter=",")
