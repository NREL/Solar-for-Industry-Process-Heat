# -*- coding: utf-8 -*-
"""
Created on Thu May  2 16:11:59 2019

@author: cmcmilla
"""

import pandas as pd
import seaborn as sns
import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import mecs_table5_2_formatting as t52_format
import itertools as it

#%%
class results_figures:
    
    def __init__(self, context='paper'):
        
        def get_regions(df, df_column):
            """
            Returns dataframe with region values.
            """
            
            df['Region'] = np.nan
            
            df['Region'].update(
                df.loc[
                    df[df_column].apply(lambda x: type(x)) ==str, 'Total'
                    ].apply(lambda x: x.split(' Census Region')[0])
                    )
        
            df.Region.fillna(method='ffill', inplace=True)
            
            df.replace({'Total United States': 'United States'},
                       inplace=True)
            
            return df
        
        self.url_3_2 =\
            'https://www.eia.gov/consumption/manufacturing/data/'+\
            '2014/xls/table3_2.xlsx'
        
        self.heat_enduses = ['Process Heating',
                             'CHP and/or Cogeneration Process',
                             'Conventional Boiler Use']
        
        sns.set(context=context, style='whitegrid', palette='Oranges')
        
        # Import MECS 2014 data for fuel use by NAICS and Region
        self.mecs3_2 =pd.read_excel(
                self.url_3_2, sheet='Table 3.2', index_col=None, skiprows=10,
                skipfooter=14
                ) 
        
        self.mecs3_2.dropna(axis=0, how='all', inplace=True)

        self.mecs3_2.iloc[:, 0].fillna(method='ffill', inplace=True)

        self.mecs3_2.reset_index(inplace=True, drop=True)
    
        self.mecs3_2.columns = ['NAICS', 'NAICS_desc', 'Total', 
                            'Net_electricity', 'Residual_fuel_oil',	
                            'Diesel', 'Natural_gas', 'LPG_NGL',	'Coal',
                            'Coke_and_breeze', 'Other']
        
        self.mecs3_2.replace(
                to_replace={'*': 0.49, '--': 0, 'Q': np.nan,'W': np.nan},
                value=None, inplace=True
                )
        
        self.mecs3_2 = get_regions(self.mecs3_2, 'Total')
                    
        self.mecs3_2.dropna(thresh=5, axis=0, inplace=True)
        
        self.mecs5_2 = t52_format.table5_2(2014).eu_table
        
    
    def align_naics_mecs(self, county_energy):
        """
        Align county-level 6-digit NAICS codes to various n-digit NAICS codes
        usedin 2014 MECS.
        Returns dataframe with energy sums matched to MECS n-digit NAICS codes
        by Census Region.
        """
    
        fuel_types = ['Coal', 'Coke_and_breeze', 'Diesel', 'LPG_NGL',
                      'Natural_gas', 'Other', 'Residual_fuel_oil', 'Total']
    
        mecs_naics = pd.DataFrame(self.mecs3_2.NAICS.unique(),
                                  columns=['mecs_naics'])
        
        mecs_naics = mecs_naics.astype(int)
    
        mecs_naics.loc[:, 'Nn'] = \
            mecs_naics.mecs_naics.apply(lambda x: len(str(x)))
            
        if type(county_energy) == pd.core.frame.DataFrame:
            
            county_naics = pd.DataFrame(county_energy.naics.unique(), 
                                        columns=['naics'])
        else:
            
            county_naics = pd.DataFrame(county_energy.naics.unique().compute(), 
                            columns=['naics'])
        
        county_naics = pd.concat(
                [county_naics.naics.apply(
                        lambda x: int(str(x)[0:n])
                        ) for n in range(3, 7)], axis=1
            )
    
        county_naics.columns = ['N' + str(n) for n in range(3, 7)]
        
        county_naics['naics'] = county_naics.N6
        
        if type(county_energy) == pd.core.frame.DataFrame:
        
            county_energy_grpd = county_energy.reset_index().groupby(
                        ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_FT'],
                        as_index=False
                        ).MMBtu_TOTAL.sum()

        else:
    
            county_energy_grpd = county_energy.reset_index().compute().groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_FT'],
                as_index=False
                ).MMBtu_TOTAL.sum()          
            
        county_energy_grpd = county_energy_grpd.set_index('naics').join(
                    county_naics.set_index('naics')
                    )
        
        county_energy_grpd.reset_index(inplace=True)
        
        county_mecs_formatted = pd.DataFrame()
    
        for n in range(3, 7):

            county_mecs_formatted = county_mecs_formatted.append(
                    pd.merge(county_energy_grpd,
                             mecs_naics[mecs_naics.Nn == n], how='inner',
                             left_on='N'+str(n), right_on='mecs_naics'),
                    ignore_index=True
                    )
        
        county_mecs_formatted.drop(['N3', 'N4', 'N5', 'N6'], axis=1,
                                   inplace=True)
    
#        county_mecs_formatted.replace({'Midwest': 'Midwest Census Region', 
#            'West': 'West Census Region', 'South': 'South Census Region',
#            'Northeast': 'Northeast Census Region', np.nan: 'Total United States'},
#            inplace=True)
    
        county_mecs_formatted.rename(columns={'MECS_Region': 'Region',
                                              'mecs_naics': 'NAICS',},
                                     inplace=True)
    
        county_mecs_formatted.Region.fillna('United States', inplace=True)
        
        county_mecs_formatted.set_index(['Region', 'NAICS'], inplace=True)
        
        county_mecs_formatted.sort_index(inplace=True)
        

#            county_merged = pd.merge(
#                county_2014, mecs_naics[mecs_naics.Nn == n], how='inner',
#                left_on='N' + str(n), right_on='naics'
#                )
#    
#            county_grouped = \
#                county_merged.groupby(
#                    ['MECS_Region', 'N' + str(n)], as_index=False
#                    )[fuel_types].sum()
#    
#            county_nformatted = county_nformatted.append(
#                county_grouped, ignore_index=True
#                )
#    
#            county_nformatted = \
#                county_nformatted.append(
#                    county_grouped.groupby(
#                        'N' + str(n), as_index=False
#                        )[fuel_types].sum(), ignore_index=True
#                    )
#    
#        county_nformatted.loc[:, 'NAICS'] = \
#            county_nformatted[['N3', 'N4', 'N5', 'N6']].sum(axis=1)
#    
#        county_nformatted.loc[:, 'NAICS'] = county_nformatted.NAICS.astype(np.int)
#    
#        county_nformatted.drop(['N3', 'N4', 'N5', 'N6'], axis=1, inplace=True)
#    
#        county_nformatted.replace({'Midwest': 'Midwest Census Region', 
#            'West': 'West Census Region', 'South': 'South Census Region',
#            'Northeast': 'Northeast Census Region', np.nan: 'Total United States'},
#            inplace=True)
#    
#        county_nformatted.rename(columns={'MECS_Region': 'Region'}, inplace=True)
#    
#        county_nformatted.set_index(['Region', 'NAICS'], inplace=True)
    
        return county_mecs_formatted
#%%
    def compare_mecs(self, county_mecs_formatted):
        """
        
        """
        
        county_pivot = county_mecs_formatted.pivot_table(
                index=['Region', 'NAICS'], columns=['MECS_FT'],
                values='MMBtu_TOTAL', aggfunc='sum'
                )
        
        # Convert from MMBtu to TBtu
        county_pivot = county_pivot.divide(10**6)
        
        county_pivot['Total'] = county_pivot.sum(axis=1)
        
        # Absolute comparison
        compare_abs = county_pivot.subtract(
                self.mecs3_2.set_index(['Region', 'NAICS']))
        
        # Relative comparison
        # Need to mask instances where county data estimate an energy value
        # that is not estimated by MECS.
        mask = (county_pivot > 0) & (self.mecs3_2 == 0)
        
        mecs_masked = self.mecs3_2.set_index(['Region', 'NAICS']).mask(
                mask, np.nan
                )
        
        compare_rel = compare_abs.divide(
                 self.mecs3_2.set_index(['Region', 'NAICS'])
                 )
        
        
    def 

    def enduse_by_empsize(self, energy_enduse_dd):
        """
        Category plot
        """
        
        plot_data = energy_enduse_dd[energy_enduse_dd.End_use.isin(
                energy_enduse_dd.heat_enduses
                )].groupby(
                    ['Emp_Size', 'End_use']
                    ).MMBtu.sum().reset_index().compute()

        g = sns.catplot(
                x='End_use', y='MMBtu', hue='Emp_Size', kind='bar',
                data=plot_data, hue_order=['n1_49', 'n50_99', 'n100_249',
                                      'n250_499','n500_999', 'n1000', 'ghgrp'],
                aspect=3, palette='Oranges'
                )
        
        g.set_xlabels('End Use')
            
        return g
        
    def mecs_compare(self, energy_dd, enduse=False):
        """
        Sum by MECS NAICS and compare to 2014 MECS. 
        
        enduse = False compares by region, NAICS, and fuel type.
        enduse = True compares by enduse, NAICS, and fuel type.
        """
        
        plot_data = energy_dd.reset_index().groupby(
                ['naics', 'MECS_Region', 'MECS_FT']
                ).MMBtu_TOTAL.sum().compute()/10**6
        
    @staticmethod
    def missing_data(mfg_energy, mfg_energy_enduse, mfg_energy_enduse_temps):
        """
        
        """
        
        
    
    @staticmethod
    def temp_load_curve(energy_temp, category_ids, realtive=False,
                        save_fig=True, save_data=False):
        """
        Plot cumulative energy use by temperature for all industries
        (default, industry=None)
        
        category is a list containing 'naics', 'MECS_FT', 'End_use', or 'all'. 
        'all' returns the temperature curve total energy for all industries.
        
        category_ids is the dictionary of specific values for each category.
        
        relative=True returns the a curve as the portion of energy use.
        
        """
        
        data_colums = ['Temp_C', 'MMBtu_Temp']
        
        for cat in category_ids.keys():
            
            data_columns.append(cat) 

        temp_data = energy_temp.reset_index()[data_columns]
        
        plot_data = pd.DataFrame()
        
        def naics_data(temp_data, category_ids['naics']): 
        
            def n_naics_grouper(temp_data, len_naics, grp2=None):
                """
                Return data groupd by naics and optional second field.
                """
                
                temp_data['naics_n'] = temp_data.naics.apply(
                    lambda x: int(str(x)[0:len_naics])
                    )
                
                if grp2 == None:
                    grpd_data = temp_data.reset_index().groupby('naics_n')
                    
                else:
                    
                    grpd_data = temp_data.reset_index().groupby(
                            ['naics_n', grp2]
                            )
    
                return grpd_data
        
        
            if any([type(x)==dict for x in category_ids['naics']]):
                
                grp2 = list(it.compress(
                        category_ids['naics'],
                        [type(x)==dict for x in category_ids['naics']]
                        ))                    

            naics_lens = {}
            
            for x in category_ids['naics']:
                
                if len(str(x)) in naics_lens.keys():
                    
                    naics_lens[len(str(x))].append(x)
                    
                else:
                    
                    naics_lens[len(str(x))] = []
                    
                    naics_lens[len(str(x))].append(x)
            
            for k in naics_lens.keys():
                
                naics_data = n_naics_grouper(temp_data, k)
                    
                plot_data = plot_data.append(
                    pd.concat(
                        [grpd_data.get_group(v).groupby(
                                'Temp_C', as_index=False
                                ).MMBtu_Temp.sum() for v in naics_lens[k]],
                        axis=0, ignore_index=True
                        ), ignore_index=True, sort=True
                    )
            
            
                    
                    

        if 'MECS_FT' in cateogry_ids.keys():
            
            ft_data = temp_data.groupby('MECS_FT')
            
            plot_data = plot_data.append(
                    pd.concat(
                        [ft_data.get_group(ft).groupby(
                            'Temp_C', as_index=False
                            ).MMBtu_Temp.sum() for ft in category_ids['MECS_FT']],
                        axis=0, ignore_index=True
                        ), ignore_index=True, sort=True
                    )
                
        if 'End_use' in cateogry_ids.keys():
            
            eu_data = temp_data.groupby('End_use')
            
            plot_data = plot_data.append(
                    pd.concat(
                        [eu_data.get_group(ft).groupby(
                            'Temp_C', as_index=False
                            ).MMBtu_Temp.sum() for ft in category_ids['End_use']],
                        axis=0, ignore_index=True
                        ), ignore_index=True, sort=True
                    )
        
        if 'all' in category_ids.keys():
            
            plot_data = plot_data.groupby(
                    'Temp_C', as_index=False
                    ).MMBtu_Temp.sum()
            
            plot_data.sort_values('Temp_C', ascending=True, inplace=True)
        
            plot_data['Cuml_TBtu'] = plot_data.MMBtu_Temp.cumsum()/10**6
            
            filename = "temp_cuve_all.png"
            
        else:
            
            plot_all_ind = plot_data.groupby(
                    'Temp_C', as_index=False
                    ).MMBtu_Temp.sum()
            
            plot_all_ind.sort_values('Temp_C', ascending=True, inplace=True)
        
            plot_all_ind['Cuml_TBtu'] = plot_all_ind.MMBtu_Temp.cumsum()/10**6
            
            plot_data = pd.concat(
                    [n_naics_grouper(plot_data, n) for n in industry],
                    axis=0)
            
            f, ax = plt.subplots(figsize=(8,8))

            sns.lineplot(y='Cuml_TBtu', x='Temp_C',
                         data=test.reset_index(), hue='naics',
                         )
            
            ax.set_ylabel('Cumulative Enenergy (TBtu)')
            
            ax.set_xlabel('Temperature (degrees C)')
            
            f.savefig(filename, bbox_inches='tight', dpi=100)
        
        #252525
            
        f, ax = plt.subplots(figsize=(8,8))

        sns.lineplot(y='Cuml_TBtu', x='Temp_C', data=plot_data, sizes=2,
                     color='#f03b20')
        
        ax.set_ylabel('Cumulative Enenergy (TBtu)')
        
        ax.set_xlabel('Temperature (degrees C)')
        
        f.savefig(filename, bbox_inches='tight', dpi=100)
            
    def summarize_county():
        
        # Sums est_count. need to get count of unique 
        test = mfg_energy_enduse_temps[
                mfg_energy_enduse_temps.COUNTY_FIPS == 1001
                ].reset_index().groupby(
                ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT',
                 'Temp_C']
                )[['est_count', 'MMBtu_Temp']].sum()
            
        test.sort_values('MMBtu_Temp', ascending=False, inplace=True)
        
    def largest_ind(self, energy_enduse_dd, n=5):
        """
        Box and whiskers
        """
        
        # Determine largest n industries by energy
        largest = energy_enduse_dd[
                energy_enduse_dd.End_use.isin(energy_enduse_dd.heat_enduses)
                ].groupby('naics').MMBtu.sum().compute().sort_values(
                    ascending=False
                    ).index[0:n]
            
        plot_data = energy_enduse_dd.data[
                (energy_enduse_dd.data.End_use.isin(
                        energy_enduse_dd.heat_enduses)
                        ) & (energy_enduse_dd.data.naics.isin(
                                largest
                                ))
            ].groupby(
                ['COUNTY_FIPS', 'naics']
                ).MMBtu.sum().reset_index().compute()
            
        f, ax = plt.subplots(figsize=(10, 8))
        
        ax.set_xscale("log")
            
        sns.boxplot(y='naics', x='MMBtu', data=plot_data, order=largest,
                        palette='colorblind', orient='h')
        
        sns.stripplot(x='MMBtu', y='naics', data=plot_data, color=".3",
                      orient='h', order=largest, size=3)

        sns.despine(left=True)
        
        ax.set_ylabel('')
        
        ax.xaxis.grid(True)

        ax.set_yticklabels(['Refining', 'Paperboard Mills', 'Paper Mills',
                            'Iron & Steel', 'All Other Org Chem'])
        
        #g = sns.swarmplot(y='naics', x='MMBtu', data=plot_data)
        
        return f
    
    

    
    