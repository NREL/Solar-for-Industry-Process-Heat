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
        self.mecs3_2 = pd.read_excel(
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
        
        self.mecs3_2 = pd.DataFrame(
                self.mecs3_2[self.mecs3_2.NAICS_desc != 'Total']
                )
        
        self.mecs3_2.replace(
                to_replace={'*': 0.49, '--': 0, 'Q': np.nan,'W': np.nan},
                value=None, inplace=True
                )
        
        self.mecs3_2 = get_regions(self.mecs3_2, 'Total')
                    
        self.mecs3_2.dropna(thresh=5, axis=0, inplace=True)
        
        # Subtract net electricity from total because the comparison
        # is with combustion fuels.
        self.mecs3_2.Total.update(
                self.mecs3_2.Total.subtract(self.mecs3_2.Net_electricity)
                )
        
        self.mecs3_2.drop('Net_electricity', axis=1, inplace=True)
        
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
        
        compare_abs = compare_abs.mask(mask)
        
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
        
        
 #%%     
   # @staticmethod
    def temp_load_curve(energy_temp, category_ids, relative=False,
                        save_fig=True, save_data=False, filename=None):
        """
        Plot cumulative energy use by temperature for all industries
        (default, industry=None)
        
        category is a list containing 'naics', 'MECS_FT', 'End_use', or 'all'. 
        'all' returns the temperature curve total energy for all industries.
        
        category_ids is the dictionary of specific values for each category.
        
        relative=True returns the a curve as the portion of energy use.
        
        exampes:
        category_ids={'all':'all'} returns temp-load curve for all
        manufacturing industries.
        
        category_ids={'naics':[311, 312, 331]}
        
        category_ids={}
        
        """
        
        sns.set(context='talk', style='whitegrid')
        
        data_columns = ['Temp_C', 'MMBtu_Temp']
        
        for cat in category_ids.keys():
            
            data_columns.append(cat)
            
        if energy_temp.index.names != [None]:
            
            energy_temp.reset_index(inplace=True)
        
        plot_data = pd.DataFrame()
        
        def naics_data(energy_temp, category_ids): 
            
            plot_data = pd.DataFrame()
        
            def n_naics_grouper(temp_data, len_naics):
                """
                Return data groupd by naics.
                """
                
                data = temp_data.copy(deep=True)
                
                data['naics'] = data.naics.apply(
                    lambda x: int(str(x)[0:len_naics])
                    )

                grpd_data = data.groupby('naics')

                return grpd_data
        
            naics_lens = {}

            for x in category_ids['naics']:
                
                if type(x) == dict:
                    
                    x = int(list(x.keys())[0])
                
                if len(str(x)) in naics_lens.keys():
                    
                    naics_lens[len(str(x))].append(x)
                    
                else:
                    
                    naics_lens[len(str(x))] = []
                    
                    naics_lens[len(str(x))].append(x)

            if any([type(x)==dict for x in category_ids['naics']]):
                
                grp2 = list(it.compress(
                        category_ids['naics'],
                        [type(x)==dict for x in category_ids['naics']]
                        ))

                for g in grp2:
                    
                    len_naics = len(list(g.keys())[0])
                    
                    grp2_cat = list(g.values())[0]
                    
                    all_grp2.append(grp2)
                    
                    naics_data = n_naics_grouper(energy_temp[data_columns],
                                                 len_naics)
                        
                    plot_data = plot_data.append(
                        pd.concat([naics_data.get_group(v).groupby(
                            ['naics', grp2_cat, 'Temp_C'], as_index=False
                            ).MMBtu_Temp.sum() for v in naics_lens[len_naics]],
                            axis=0, ignore_index=True), ignore_index=True,
                        sort=True
                        )
            
                    data_columns.remove(grp2_cat)

            for k in naics_lens.keys():

                naics_data = n_naics_grouper(energy_temp[data_columns], k)

                plot_data = plot_data.append(
                    pd.concat([naics_data.get_group(v).groupby(
                        ['naics', 'Temp_C'], as_index=False
                        ).MMBtu_Temp.sum() for v in naics_lens[k]],
                        axis=0, ignore_index=True), ignore_index=True,
                    sort=True
                    )
            
            # Drop total energy values for NAICS without further disaggregation
            drop_naics = plot_data.dropna()['naics'].unique()
            
            plot_data = pd.concat([
                    plot_data[~plot_data.naics.isin(drop_naics)],
                    plot_data.dropna()
                    ], axis=0, ignore_index=True)
            
            grp2_all = ['naics']
            
            for cat in set(plot_data.columns).difference(
                    set(['MMBtu_Temp', 'Temp_C', 'naics'])
                    ):
                
                grp2_all.append(cat)
            
            plot_data.set_index(grp2_all, inplace=True)
            
            naics_plot_data = pd.DataFrame()
            
            for n in plot_data.index.unique():
                
                data = plot_data.xs(n).sort_values('Temp_C')
                
                data['Cuml_TBtu'] = data.MMBtu_Temp.cumsum()/10**6
                
                if relative == True:
                    
                    data['Cuml_TBtu'].update(
                        data['Cuml_TBtu'].divide(
                            data.MMBtu_Temp.sum()/10**6
                            )*100
                        )
                        
                data['naics'] = n
                
                naics_plot_data = naics_plot_data.append(
                        data, ignore_index=True, sort=False
                        )
                
            naics_plot_data.drop('MMBtu_Temp', axis=1, inplace=True)
            
            return naics_plot_data
    
        def fueltype_enduse_data(energy_temp, cat_id):
            
            plot_data = pd.DataFrame()
            
            ft_data = energy_temp.groupby(cat_id)
            
            for g in ft_data.groups:
                
                data = ft_data.get_group(g)
                
                data = data.groupby('Temp_C', as_index=False).MMBtu_Temp.sum()
                
                data[cat_id] = g
                
                data.sort_values('Temp_C', ascending=True, inplace=True)
                
                data['Cuml_TBtu'] = data.MMBtu_Temp.cumsum()/10**6
                
                if relative == True:
                    
                    data['Cuml_TBtu'].update(
                            data.Cuml_TBtu.divide(
                                    data.MMBtu_Temp.sum()/10**6
                                    )*100
                            )
                
                data.drop('MMBtu_Temp', axis=1, inplace=True)
                
                plot_data = plot_data.append(data, ignore_index=True,
                                             sort=True)

            return plot_data

        
        if 'naics' in category_ids.keys():
            
            plot_data = plot_data.append(naics_data(energy_temp, category_ids))

        if ('MECS_FT' in category_ids.keys()) | \
            ('End_use' in category_ids.keys()):
                
            for ids in set(category_ids.keys()).difference(['naics']):
            
                plot_data = plot_data.append(
                        fueltype_enduse_data(energy_temp, ids)
                        )
            
        if 'all' in category_ids.keys():
            
            plot_data = energy_temp.groupby(
                    'Temp_C', as_index=False
                    ).MMBtu_Temp.sum()
            
            plot_data.sort_values('Temp_C', ascending=True, inplace=True)
            
            if relative == True:
        
                plot_data['Cuml_TBtu'] = plot_data.MMBtu_Temp.cumsum()
                
                plot_data['Cuml_TBtu'].update(
                        plot_data['Cuml_TBtu'].divide(
                                plot_data.MMBtu_Temp.sum()
                                )*100
                        )
            else:
                
                plot_data['Cuml_TBtu'] = plot_data.MMBtu_Temp.cumsum()/10**6
            
            f, ax = plt.subplots(figsize=(8,8))

            sns.lineplot(y='Cuml_TBtu', x='Temp_C', data=plot_data, sizes=2,
                         color='#f03b20')
                         
            ax.set_title('All Manufacturing Industries')
            
            if relative == True:
            
                ax.set_ylabel('Cumulative Energy (%)')
                
            else:
                
                ax.set_ylabel('Cumulative Energy (TBtu)')
            
            ax.set_xlabel(u'Temperature (\u00B0C)')
            
            if save_fig == True:
                
                f.savefig(filename, bbox_inches='tight', dpi=100)
                
            return
        
        if 'naics' in plot_data.columns:
            
            plot_data['naics'] = plot_data.naics.astype(str)
        
        if relative == True:
                    
            ylabel = 'Cumulative Energy (%)'
            
        else:
            
            ylabel = 'Cumulative Energy (TBtu)'

        hue_column = list(set(plot_data.columns).difference(
                set(['Cuml_TBtu', 'Temp_C'])
                ))[0]
        
        f, ax = plt.subplots(figsize=(8,8))
        
        if hue_column == 'naics':

            sns.lineplot(y='Cuml_TBtu', x='Temp_C', data=plot_data, 
                         hue=hue_column, sizes=[3], palette=sns.color_palette(
                                 'deep', len(plot_data[hue_column].unique())
                                 ), style=hue_column, markers=True)

        else:

            sns.lineplot(y='Cuml_TBtu', x='Temp_C', data=plot_data, 
                         hue=hue_column, sizes=[3], palette=sns.color_palette(
                                 'deep', len(plot_data[hue_column].unique())
                                 ), markers=True)
    
        ax.set_ylabel(ylabel)
        
        ax.set_xlabel(u'Temperature (\u00B0C)')
        
        f.savefig(filename, bbox_inches='tight', dpi=100)
        
            

#%%          
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
    
    

    
    