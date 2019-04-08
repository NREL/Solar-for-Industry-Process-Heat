# -*- coding: utf-8 -*-
"""
Created on Fri Jan 13 13:45:19 2017
@author: ookie
Modified on Thur Feb 9 16:09:00 2017 by colin
"""

import numpy as np
import os
import itertools as it
import pandas as pd

class IPF:
    
    def __init__(self, year, table3_2, table3_3):
        
        self.__location__ =  os.path.join('../', 'calculation_data/')

        self.year = year
        
        self.naics_df = table3_2
        
        self.emply_df = table3_3
        
        self.colDict = {'regions': ['Northeast', 'Midwest', 'South', 'West'],
           'energy': ['Net_electricity', 'Residual_fuel_oil', 'Diesel',
                      'Natural_gas', 'LPG_NGL', 'Coal',
                      'Coke_and_breeze','Other'],
           'employment': ['Under 50', '50-99','100-249', '250-499',
                          '500-999', '1000 and Over'],
           'value': ['Under 20', '20-49', '50-99', '100-249', '250-499',
                     '500 and Over']}

        def combine(self, columns):
            """
            Takes values in the dictionary above and creates a list of all
            combinations
            """
        
            labels = [self.colDict[x] for x in columns]
    
            labels = list(it.product(*labels))
    
            output = []
    
            for i,label in enumerate(labels):
    
                output.append('_'.join(label))
    
            return output
        
        #create column headings that have combinations of regions and energy 
        #carriers
        self.headings = combine(self, ['regions', 'energy'])
        
        self.headings_all = combine(self,
                                    ['regions', 'energy', 'employment'])

    def ipf2D_calc(self, seed, col, row):
        """
        Core two-dimensional iterative proportional fitting algorithm.
        col matrix should have dimensions of (m,1)
        row matrix should have dimensions of (1,n)
        seed matrix should have dimensions of (m,n)
        """

        col_dim = col.shape[0]
        row_dim = row.shape[1]

        for n in range(3000): #set maximumn number of iterations
            error = 0.0
            #faster 'pythonic(?)' version
            sub = seed.sum(axis=1,keepdims=True)
            sub = col / sub
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:, np.newaxis],row_dim,axis=1)
            seed = seed*sub
            diff = (seed.sum(axis=1, keepdims=True)-col)
            diff = diff*diff
            error += diff.sum()
            sub = seed.sum(axis=0, keepdims=True)
            sub = row / sub
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:, np.newaxis],col_dim,axis=1)
            sub = sub.transpose()
            seed = seed*sub
            diff = (seed.sum(axis=0, keepdims=True)-row)
            diff = diff*diff
            diff = diff.sum()
            error = np.sqrt(error)
            if error < 1e-15: break
        #report error if max iterations reached
        if error > 1e-13: print("Max Iterations ", error)
        return seed
    
    def mecs_ipf(self, seed_df):
        """
        Set up and run 2-D IPF to estimate MECS fuel use by industry,
        region, fuel type, and employement size class.
        naics_df == MECS table 3.2
        emply_df == MECS table 3.3
        """
        
        seed_shop = seed_df.copy(deep=True)
        
        seed_shop.set_index(['region', 'Unnamed: 0'], inplace=True)

        seed_shop = seed_shop.T

        seed_shop_dict = {}
    
        # Iterate through all of the fuel types
        first = True

        for r in range(0, len(self.colDict['energy'])):

            counter = 6 * r
            
            fuel = self.colDict['energy'][r]
            
            for reg in self.colDict['regions']:
                                
                seed_shop_dict[reg] = seed_shop[reg].iloc[
                        :, (0 + counter):(6 + counter)
                        ]

                print(reg, fuel)
                
                col = self.naics_df[self.naics_df.region==reg][fuel].values
                
                row = self.emply_df[
                        (self.emply_df.region==reg) &
                        (self.emply_df.Data_cat=='Employment_size')
                        ][fuel].values

                col = np.array([col])

                row = np.array([row])

                col = np.transpose(col)

                seed = np.array(seed_shop_dict[reg].iloc[0:81,:])
                
                seed = seed.astype(float)

                col = col.astype(float)

                row = row.astype(float)
                
                if first: 
                    
                    naics_emply = self.ipf2D_calc(seed, col, row)
    
                else: 
                    naics_emply = np.hstack(
                            (naics_emply, self.ipf2D_calc(seed, col, row))
                            )
    
                first = False

        naics_emply = np.hstack((
            self.naics_df[
                    self.naics_df.region=='West'
                    ].iloc[:, 0].values.reshape(81,1), naics_emply
            ))
                    
        self.headings_all.insert(0, 'naics')

        naics_emply = np.vstack((self.headings_all, naics_emply))

        filename = 'mecs_' + str(self.year) + \
            '_ipf_results_naics_employment.csv'
        
        np.savetxt(self.__location__ + filename, naics_emply, fmt='%s',
                   delimiter=",")
        
