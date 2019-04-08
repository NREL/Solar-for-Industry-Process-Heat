# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 14:06:54 2019

@author: cmcmilla
"""
import pandas as pd
import numpy as np
import os

class table5_2:
    
    def __init__(self, year):
        
        self.base_url = 'https://www.eia.gov/consumption/manufacturing/data/'
        
        self.year = year
        
        if self.year == 2010:
            
            skipfooter = 43
            
            file_url = '2010/xls/table5_2.xls'
            
        else:
            
            skipfooter = 21
            
            file_url = '2014/xls/table5_2.xlsx'

        table_url = base_url + file_url
        
        self.fuels = ['total', 'net_electricity', 'fuel_oil', 'diesel',
                      'natural_gas', 'NGL_HGL', 'coal', 'other']
        
        self.eu_table = pd.read_excel(
                table_url, sheet_name='Table 5.2', skiprows=15,
                skipfooter=skipfooter, header=None,
                names=['naics', 'end_use', 'total', 'net_electricity',
                       'fuel_oil', 'diesel','natural_gas', 'NGL_HGL', 'coal',
                       'other']
                )

        # Fill missing data
        self.eu_table.naics.fillna(method='ffill', inplace=True)
        
        self.eu_table.dropna(thresh=3, axis=0, inplace=True)
        
        self.eu_table.replace({'--': 0, '  311 - 339': 31}, inplace=True)
        
        self.eu_table['naics'] = self.eu_table.naics.astype(int)
        
        #self.eu_table['n_naics'] = self.eu_table.naics.apply(lambda x: len(str(x)))
        
        self.eu_table['enduse_cat'] = self.eu_table.end_use.apply(
                lambda x: x.split('  ')[0]
                )
        
        self.eu_table['enduse_total'] = ~self.eu_table.enduse_cat.isin([''])
        
        self.eu_table.enduse_cat.replace({'': np.nan}, inplace=True)
        
        self.eu_table.update(
                self.eu_table[self.eu_table.enduse_total == False].replace(
                    {'*': 0.01}
                    )
                )
        
        self.eu_table.enduse_cat.fillna(method='ffill', inplace=True)
        
       # fix missing 'End Use Not Reported' row for Gypsum (32740)
        if self.year == 2014:
 #%%              
            gypsum = pd.DataFrame([[327420,'End Use Not Reported',0.01,0,0,0,0,
                                   0,0,0.01,6,'End Use Not Reported',False]],
                                    columns=self.eu_table.columns)
                                   
            self.eu_table = self.eu_table.append(gypsum, ignore_index=True)     
            
 
        #self.eu_table['total'] = self.eu_table.total.astype(int)
        
        
        #Also: loop through TOTAL FUEL COSNUMPTION ROWS. If count(Q, *) =1, then
        # if col == total, q = sum(remaining); if col != total, q = total - sum(remaining)
        # Also check if not_total == False rows are correctly filling in all instances where there's only one missing value
        
        
        self.eu_table.set_index(['naics', 'enduse_cat', 'enduse_total', 'end_use'],
                               inplace=True)
        
        self.eu_table.sort_index(inplace=True)
        
        self.eu_table.update(self.eu_table.loc[
                ((self.eu_table.index.get_level_values('naics')),
                 'End Use Not Reported',), ('net_electricity'):('other')
                ].replace(
                    {'*': 0.01, '  Facility HVAC (g)': '  Facility HVAC'}
                    )()
        
        # Calcualte missing values (* and Q) for enduse category totals
        for i in self.eu_table.xs(
                'TOTAL FUEL CONSUMPTION', level='enduse_cat'
                ).index:
            
            i = list(i)
            
            i.insert(1, 'TOTAL FUEL CONSUMPTION')
            
            if self.eu_table.loc[tuple(i), :].isin(['*', 'W', 'C']).any():
                
                continue
            
            else:
            
                q_test = self.eu_table.loc[tuple(i), :].isin(['Q'])
                
                if sum(q_test) == 1:
                    
                    q_col = q_test.multiply(self.eu_table.columns)
                    
                    q_col = q_col[q_col != ""][0]
                    
                    if q_col == 'total':
                        
                        q_value = self.eu_table.loc[
                            i, ('net_electricity'):('other')
                            ].sum()
                            
                    else:
        
                        not_str = ~q_test
                        
                        not_str = not_str.multiply(
                                self.eu_table.loc[tuple(i), :]
                                )
    
                        q_value =\
                            not_str['total'] - \
                                (not_str[not_str!=""].sum() - not_str['total'])
                                
                    if q_value >=0:
                        
                        self.eu_table.loc[tuple(i), q_col] = q_value
                        
                    else:
                        
                        self.eu_table.loc[tuple(i), q_col] = 0

        for naics in self.eu_table.index.get_level_values('naics'):
            
            totals = self.eu_table.xs(
                    naics, level='naics'
                    ).xs(True, level='enduse_total')
            
            for f in self.fuels:
                
                q_test = totals[f][totals[f]=='Q']
                
                if (len(q_test) == 1) &\
                    (all([type(x) != str for x in totals[f][totals[f]!='Q']])):
                    
                    q_row = q_test.index
                    
                    if q_row[0][0] == 'TOTAL FUEL CONSUMPTION':
                        
                        q_value = totals[f][totals[f]!='Q'].sum()
                        
                        if q_value < 0:
                            
                            q_value = 0
                        
                    else:
    
                        q_value = \
                            totals[f].xs(
                                'TOTAL FUEL CONSUMPTION', level='enduse_cat'
                                ).subtract(totals[f][totals[f]!='Q'].sum() -\
                                    totals[f].xs('TOTAL FUEL CONSUMPTION',
                                                 level='enduse_cat'))
                    
                        if q_value[0] < 0:
                            
                            q_value = 0
                            
                        else:
                            
                            q_value = q_value[0]
                    
                    self.eu_table.loc[
                        (naics, q_row[0][0], True, q_row[0][1]), f
                        ] = q_value
                
                else:
                    
                    continue
                                                       
            for eu_cat in ['Direct Uses-Total Nonprocess',
                           'Direct Uses-Total Process',
                           'Indirect Uses-Boiler Fuel']:
                
                index_values = (naics, eu_cat, True, eu_cat)
        
                for f in self.fuels:
                    
                    if (self.eu_table.loc[index_values, f] == '*') &\
                       ('Q' not in self.eu_table.loc[(naics,eu_cat,False,), f].values):
                        
                        self.eu_table.loc[index_values, f] = \
                            self.eu_table.loc[(naics, eu_cat, False,), f].sum()
                            
                    if (self.eu_table.loc[index_values, f] == 'Q') &\
                       ('Q' not in self.eu_table.loc[(naics,eu_cat,False,), f].values):
    
                        self.eu_table.loc[index_values, f] = \
                            self.eu_table.loc[(naics, eu_cat, False,), f].sum()
                                
    #                if (self.eu_table.loc[index_values, f] in ['Q', '*']) &\
    #                   ('Q' in self.eu_table.loc[(naics,eu_cat,True,), f].values):
    #                       
    #                        continue
        
            # Fill in missing values for total consumption by fuel and 
            for f in ['total', 'net_electricity', 'fuel_oil', 'diesel',
                          'natural_gas', 'NGL_HGL', 'coal', 'other']:
            
    #            if (self.eu_table.loc[(naics, 'TOTAL FUEL CONSUMPTION',False,
    #                              'TOTAL FUEL CONSUMPTION'), f] in ['Q', '*']) &\
    #               (self.eu_table.loc[(naics, 'End Use Not Reported',False,
    #                             'End Use Not Reported'), f] in ['Q', '*']):
    #                                
    #                            continue
                            
                if (self.eu_table.loc[(naics, 'TOTAL FUEL CONSUMPTION',True,
                                  'TOTAL FUEL CONSUMPTION'), f] in ['Q', '*']) &\
                   (self.eu_table.loc[(naics, 'End Use Not Reported',True,
                                 'End Use Not Reported'), f] not in ['Q', '*']):
    
                    try:
                        
                        self.eu_table.loc[
                            (naics, ('Direct Uses-Total Nonprocess',
                                     'Direct Uses-Total Process',
                                     'Indirect Uses-Boiler Fuel',
                                     'End Use Not Reported'), True,
                            ('Direct Uses-Total Nonprocess',
                             'Direct Uses-Total Process',
                             'Indirect Uses-Boiler Fuel','End Use Not Reported')),
                            f].sum()
                
                    except TypeError:
    
                        continue
                    
                    else:
        
                        self.eu_table.loc[(naics, 'TOTAL FUEL CONSUMPTION',
                                      True, 'TOTAL FUEL CONSUMPTION'),f] =\
                            self.eu_table.loc[
                                (naics, ('Direct Uses-Total Nonprocess',
                                         'Direct Uses-Total Process',
                                         'Indirect Uses-Boiler Fuel',
                                         'End Use Not Reported'), True,
                                ('Direct Uses-Total Nonprocess',
                                 'Direct Uses-Total Process',
                                 'Indirect Uses-Boiler Fuel',
                                 'End Use Not Reported')), f].sum()
#%%      

        def q_fill(enduse_total):
            """
            Method for filling in 'Q' (data withheld due to standard error) values.
            Must first implement for end use category totals (enduse_total == True), 
            then for individual end use categories (enduse_total == False) 
            """
            
            if self.eu_table.index.names != [None]:
        
                self.eu_table.reset_index(inplace=True)
                
            if enduse_total == False:
                
                eu_grpd = \
                    self.eu_table[self.eu_table.enduse_total == enduse_total].groupby(
                            ['naics', 'enduse_cat']
                            )

            else:

                eu_grpd = \
                    self.eu_table[self.eu_table.enduse_total == enduse_total].groupby(['naics'])
        
            for g in eu_grpd.groups:
                
                df_grp = eu_grpd.get_group(g)
                
                vals = df_grp.isin(['Q']).sum()
                
                if vals[vals==1].empty == False:
                    
                    fuels = vals[vals==1].index
                    
                    for f in fuels:
                        
                        i = df_grp[df_grp[f] == 'Q'].index
    
                        if self.eu_table.loc[i, 'enduse_cat'].values != 'TOTAL FUEL CONSUMPTION':
                            
                            if enduse_total == True:
                                
                                update = df_grp[df_grp !='Q'][f].sum() -\
                                    df_grp[
                                        df_grp.enduse_cat=='TOTAL FUEL CONSUMPTION'
                                        ][f].values
                                            
                            else:
                                
                                try: 
                                    
                                    update = \
                                        self.eu_table[(self.eu_table.naics == g[0]) & 
                                             (self.eu_table.end_use == g[1])][f].values - \
                                                 df_grp[df_grp !='Q'][f].sum()
                                                  
                                except TypeError:
                                    
                                    continue
                                
                                else:
            
                                    update = \
                                        self.eu_table[(self.eu_table.naics == g[0]) & 
                                             (self.eu_table.end_use == g[1])][f].values - \
                                                 df_grp[df_grp !='Q'][f].sum()
                            
                                    self.eu_table.loc[i, f] = update
                        
                        else:
                            
                            continue
                    
                else:
                    
                    continue
                
            return self.eu_table

#%%    
        # fill 'Q' values first for end use category totals, then individual 
        # end use categories
        self.eu_table = q_fill(enduse_total=True)
        
        self.eu_table = q_fill(enduse_total=False)
            
    
        # Save the table for final manual filling in of data.
        self.eu_table.to_csv(os.path.join('../calculation_data/',
                             'table5_2_'+str(self.year)+'.csv'), index=False)
        

    def format_other_use(self):
        
        if self.year == 2010:
            
            byp_url = '2010/xls/table3_5.xls'
            
            bio_url = '2010/xls/table3_6.xls'
            
        if self.year == 2014:
            
            byp_url = '2014/xls/table3_5.xlsx'
            
            bio_url = '2014/xls/table3_6.xlsx'
            
        
        self.byp_table = pd.read_excel(
                self.base_url + byp_url, skiprows=13, header=None,
                names=['naics', 'naics_desc', 'total', 'blast_furnace',
                         'waste_gas', 'pet_coke', 'pulp_liq', 'wood_chips', 
                         'waste_oils'], sheet_name=0
                )
        
        self.bio_table = pd.read_excel(
                self.base_url + bio_url, skiprows=20, header=None,
                names=['naics', 'naics_desc', 'pulp_liq', 'total_bio', 
                         'ag_waste', 'wood', 'wood_res', 'wood_paper_ref'],
                sheet_name=0
                )
        
        self.byp_table.name = 'byp'
        
        self.bio_table.name = 'bio'
                    
        
        for df in [self.byp_table, self.bio_table]:
            
            df.replace({'*':0.1}, inplace=True)
            
            df.dropna(thresh=3, axis=0, inplace=True)
            
            if df.name == 'bio':
                
                other_index = df[df.naics_desc=='Other Manufacturing'].index
                
                df.loc[other_index, 'naics'] == '399'
                
            df.naics.fillna('31-33', inplace=True)
            
            df['naics'] = df.naics.apply(lambda x: x.strip())
            
            region_n = len(df)/5
            
            df['region'] = np.repeat(['us', 'northeast', 'midwest', 'south',
                                      'west'], region_n)
            
            df['n_naics'] = df.naics.apply(lambda x: len(x))
            
            total_index = df[df.naics == '31-33'].index
            
            df.loc[total_index, 'n_naics'] = 2
            
            # Export for manual formatting (i.e., filling in Q values)
            
            df.to_csv(os.path.join(
                    '../calculation_data/' + df.name + str(self.year) + '.csv'
                    ), index=False)
            

        def calculate_eu_share(eu_table, bio_table, byp_table):
            
            
            
            # calculate fuel totals by NAICS, subtracting amount
            # without a specified end use.
            
            
            
            enduse.set_index(['naics', 'end_use'], inplace=True)
            
            adj_total = enduse.xs(
                'TOTAL FUEL CONSUMPTION', level=1
                ).loc[:, 'total':].subtract(enduse.xs('End Use Not Reported',
                                                      level=1).loc[:,
                                                                   'total':],
                level=1
                    )
                
            enduse.reset_index('enduse', inplace=True)
            
            enduse_fraction = enduse.divide(adj_total, axis=1, level=0)
            
            
            # Net steam use is estimated as the difference between the
            # "Other" column of Table 5.2 and the sum of byproducts
            # (less wood chips) and biomass.
            #
            
            byp_bio = byp_table[byp_table.region == 'us'].set_index(
                'naics'
                ).total.subtract(
                    byp_table[byp_table.region == 'us'].set_index(
                            'naics'
                            ).wood_chips
                    )
            
            byp_bio = byp_bio.add(
                    bio_table[bio_table.region == 'us'].set_index(
                            'naics'
                            ).total_bio
                    )
                    
            steam_fraction = pd.read_csv(
                os.path.join(
                    '../calculation_data/'+'steam_enduse_fraction.csv'
                    )
                )
                
            steam_fraction.fillna(method='ffill', inplace=True)
            
            steam_fraction = pd.melt(steam_fraction, id_vars='naics', 
                                 var_name='end_use', value_name='other')

            steam = pd.DataFrame(
                    eu_table[eu_table.other !=0][['naics', 'other']],
                    index_col=['naics']
                    )
            
            steam = steam.other.subtract(byp_bio)
            
            #calculate steam energy by end use using breakout from
            #Energetics footprint doc
            
            
            #Separately calculate byproduct and biomass end use energy
            # using assumptions.
            #other_fraction = pd.read_csv()
            # fillna for naics
            # reindex with eu_table index (set to naics and end_use)
            # fill in nan values with 31-33
            
            
            
        
        
        
    

