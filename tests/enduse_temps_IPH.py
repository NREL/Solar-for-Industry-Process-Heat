# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 09:42:59 2019

@author: cmcmilla
"""
import pandas as pd
import os
import numpy as np
import dask.dataframe as dd
from sklearn.utils import resample


#%%
class process_temps:

    def __init__(self):

        self.datadir = 'calculation_data/'

        self.nfiles = {'sic_N02_file': '1987_SIC_to_2002_NAICS.csv',
                       'N02_N07_file': '2002_to_2007_NAICS.csv',
                       'N07_N12_file': '2007_to_2012_NAICS.csv'}

        self.temp_buckets = {'<100': (0, 99), '100-249': (100, 249),
                             '250-399': (250, 399), '400-999': (400, 999),
                             '>1000': (1000, 3000)}

        self.temp_file = 'ProcessTemps_108.csv'

        self.eu_map = {'Process Heating': 'fuel',
                  'Conventional Boiler Use': 'boiler',
                  'CHP and/or Cogeneration Process': 'boiler'}

        def ImportTemps():
            """
            Import temperature data obtained from 108 Industrial Processes.
            Requires adjusting from 1997 SIC to 2002 NAICS to 2007 NAICS to
            2012 NAICS.
            """
            def create_sic_naics_dfs(file_dir, file):

                sic_naics = pd.read_csv(
                        os.path.join('../', file_dir + file)
                        )

                sic_naics = sic_naics.iloc[:, [0, 2]]

                sic_naics.set_index(sic_naics.columns[0], inplace=True)

                return sic_naics


            def create_dict(file_dir, file):

                dict_out = dict(pd.read_csv(
                        os.path.join('../', file_dir + file)
                        ).iloc[:, [0, 2]].values)

                return dict_out

            def adj_heat_type(heat_type):
                """
                Create adjusted heat type for aggregating and assigning
                temperatures to MECS end uses.
                """
                adj_ht = np.nan

                if heat_type in ['water', 'steam']:

                    adj_ht = 'from_boiler'

                if heat_type in ['fuel']:

                    adj_ht = 'fuel'

                return adj_ht

            ndict = {}

            for k, v in self.nfiles.items():

                ndict[k[0:7]] = create_sic_naics_dfs(self.datadir, v)

            temps = pd.read_csv(
                    os.path.join('../', self.datadir + self.temp_file)
                    )

            temps.SIC.fillna(method='ffill', inplace=True)

            temps['Temp_C'] = temps.Temp_C.apply(
                lambda x: int(np.around(x))
                )

            # Remove boiler entries, which describe boiler inputs and outputs
            temps = pd.DataFrame(temps[temps.Unit_Process != 'Boiler'])

            temps['adj_HT'] = temps.Heat_type.apply(lambda x: adj_heat_type(x))

            temps.replace({'from_boiler': 'boiler'}, inplace=True)

            temps['Heat_type'] = temps.adj_HT

            temps.drop(['adj_HT'], axis=1, inplace=True)

            temps['SIC'] = temps.SIC.apply(lambda x: int(str(x)[0:4]))

#            temps['NAICS02'] = temps.SIC.map(ndict['sic_N02'])

            temps = temps.set_index('SIC').join(ndict['sic_N02']).reset_index()

#            temps['NAICS07'] = temps.NAICS02.map(ndict['N02_N07'])

            ndict['N02_N07'].index.name = '2002 NAICS'

            temps = temps.set_index('2002 NAICS').join(
                    ndict['N02_N07']
                    ).reset_index()

#            temps['NAICS12'] = temps.NAICS07.map(ndict['N07_N12'])

            ndict['N07_N12'].columns = ['NAICS12']

            temps = temps.set_index('2007 NAICS Code').join(
                    ndict['N07_N12']
                    ).reset_index()

            # Multiple entries for each SIC/NAICS; take simple mean.
            temps = temps.groupby(
                ['NAICS12', 'Unit_Process', 'Heat_type']
                )[['E_Btu', 'Temp_C']].mean()

            # Calculate energy fraction of each process by temperature
            e_totals = temps.reset_index().groupby(
                    ['NAICS12', 'Heat_type']
                    ).E_Btu.sum()

            temps.reset_index(['Unit_Process'], inplace=True)

            temps.sort_index(inplace=True)

            e_totals.sort_index(inplace=True)

            temps = temps.join(e_totals, rsuffix='_total')

            temps['Fraction'] = temps.E_Btu.divide(temps.E_Btu_total)

            temps.drop(['E_Btu_total'], axis=1, inplace=True)

            temps.reset_index(inplace=True)

            # Some NAICS are now associated with non-manufacturing due to
            # translation from SIC to NAICS
            temps = pd.DataFrame(temps[temps.NAICS12.between(300000,400000)])

            # Create 4-, and 3-digit NAICS table for matching
            temps_NAICS = pd.DataFrame(
                pd.concat(
                    [pd.Series(
                        [float(str(x)[0:n]) for x in temps.NAICS12.unique()],
                        index=temps.NAICS12.unique()
                        ) for n in [5, 4, 3]], axis=1
                    )
                )

            temps_NAICS.columns = ['N5', 'N4', 'N3']

            temps_NAICS.index.name = 'NAICS12'

            temps_NAICS.reset_index(inplace=True)

            # Merge temps and temps_NAICS for future operations by other NAICS

            temps = pd.merge(temps, temps_NAICS, on='NAICS12', how='left')

            for tb, tr in self.temp_buckets.items():

                ti = temps[temps.Temp_C.between(tr[0], tr[1])].index

                temps.loc[ti, 'Temp_Bucket'] = tb

            return temps

        self.temps = ImportTemps()


    def temp_mapping(self, MECS_NAICS, eu_energy_dd):
        """
        Map temperature to end use disggregation.
        """

        def agg_temps():
            """
            Create temperature aggregations by 3- to 5-digit NAICS codes
            to use for NAICS not covered in the original temperature survey.
            The survey covers 44 unique 6-digit NAICS; there are 305 6-digit
            manufacturing NAICS in the energy data set.

            Bootstrapping is used to estimate the temperature quartiles by
            heat type (i.e., boiler, fuel)

            """

            def bootstrap(data, iterations=10000):
                """
                Nonparametric bootstrapping for mean and standard deviation of
                data.
                """

                if len(data) == 1:

                    final_dist = np.array(np.repeat(data.values[0],3))

                else:

                    boot_median = []

                    boot_lq = []

                    boot_uq = []

                    for n in range(0, iterations):

                        boot = resample(data, replace=True, n_samples=None,
                                        random_state=None)

                        boot_median.append(np.median(boot))

                        boot_lq.append(np.percentile(boot, 25))

                        boot_uq.append(np.percentile(boot, 75))

                    final_dist = np.array(
                            [np.mean(x) for x in [boot_lq, boot_median, boot_uq]]
                            )

                return final_dist

            agg_temps = pd.DataFrame()

            for col in ['N5', 'N4', 'N3']:

                grpd = self.temps.groupby([col, 'Heat_type']).Temp_C.apply(
                        lambda x: bootstrap(x)
                        )

                grpd = pd.DataFrame.from_records(grpd, index=grpd.index)

                grpd.columns = ['lower_q', 'median', 'upper_q']

                grpd.index.names = ['TN_Match', 'Heat_type']

                grpd = pd.melt(grpd.reset_index(), id_vars=grpd.index.names,
                               var_name='Fraction', value_name='Temp_C')

                agg_temps = agg_temps.append(grpd, sort=True)

            agg_temps.replace({'lower_q': 0.25,'median': 0.5,'upper_q': 0.25},
                              inplace=True)

            return agg_temps


        def naics_matching(df_to_match, df_match_against):
            """
            Match naics between end use data set and temperature info.
            """
            match = pd.DataFrame(columns=['NAICS12', 'N5', 'N4', 'N3'])

            mask = pd.DataFrame(columns=['NAICS12', 'N5', 'N4', 'N3'])

            for col in ['NAICS12', 'N5', 'N4', 'N3']:

                match[col] = pd.Series(
                                [x in df_to_match[col].values for \
                                 x in df_match_against[col].values], name=col
                                 )
                mask[col] = \
                    df_match_against.reset_index()[col].multiply(match[col])

#                match[col] = pd.Series(
#                                [x in df_match_against['NAICS12'].values for \
#                                 x in df_to_match[col].values], name=col
#                                 )
#                mask[col] = \
#                    df_to_match.reset_index()[col].multiply(match[col])

#
#                match[col] = pd.Series(
#                                [x in df_match_against[col].values for \
#                                 x in df_to_match[col].values], name=col
#                                 )
#
#                    match = pd.concat(
#                            [match, pd.Series(
#                                    [x in df_match_against[col].values for \
#                                     x in df_to_match[col].values], name=col)],
#                            axis=1, ignore_index=True
#                            )
#                mask[col] = \
#                    df_to_match.reset_index()[col].multiply(match[col])
#
#                    mask = pd.concat(
#                            [mask, df_to_match[col].multiply(match[col])],
#                            axis=1, ignore_index=True
#                            )

            mask.replace({0:np.nan}, inplace=True)

            mask.N3.fillna(0, inplace=True)

            mask['TN_Match']= mask.apply(
                    lambda x: int(list(x.dropna())[0]), axis=1
                    )

            mask.rename(columns={'NAICS12':'N6'}, inplace=True)

            mask['NAICS12'] = df_match_against.NAICS12.values

            mask['MECS_NAICS'] = df_match_against.index.get_level_values(0)

            return mask

        eu_energy_temp_dd = eu_energy_dd.copy()

        # Calculate temperatures and associated energy use only for boiler
        # and process heating end uses.
        # Does not address industries that were not matched to a 3-digit NAICS
        eu_naics = pd.DataFrame(
                eu_energy_temp_dd['naics'].drop_duplicates().compute()
                )

        eu_naics.rename(columns={'naics':'NAICS12'}, inplace=True)

        for n in [5, 4, 3]:

            eu_naics.loc[:, 'N' + str(n)] = \
                [float(str(x)[0:n]) for x in eu_naics.NAICS12.values]

        naics_to_temp = naics_matching(
                self.temps.drop_duplicates(subset='NAICS12'), eu_naics
                )

        if 'agg_temps.csv' in os.listdir(os.path.join('../', self.datadir)):

            other_temps = pd.read_csv(
                    os.path.join('../', self.datadir+'agg_temps.csv')
                    )

        else:

            print('Bootstrapping temperatures')

            other_temps = agg_temps()

            other_temps.to_csv(
                os.path.join('../', self.datadir+'agg_temps.csv')
                )

        tn_match_fraction = pd.concat(
                [self.temps[['NAICS12', 'Heat_type', 'Temp_C', 'Fraction']],
                 other_temps], axis=0, ignore_index=True, sort=True
                 )

        tn_match_fraction.TN_Match.update(tn_match_fraction.NAICS12)

        tn_match_fraction.drop('NAICS12', inplace=True, axis=1)

        tn_match_fraction = pd.DataFrame(
                naics_to_temp[naics_to_temp.TN_Match!=0].set_index(
                'TN_Match')['NAICS12']
                ).join(tn_match_fraction.set_index('TN_Match'))

        tn_match_fraction.rename(columns={'NAICS12':'naics'}, inplace=True)

        tn_match_fraction.reset_index(inplace=True)


#        tn_match_fraction = pd.merge(
#                tn_match_fraction, naics_to_temp[['TN_Match', 'MECS_NAICS']],
#                on='TN_Match', how='left'
#                )
#
#        updated_temps = self.temps.set_index('NAICS12').join(
#                naics_to_temp.set_index('NAICS12')[['TN_Match', 'MECS_NAICS']]
#                ).reset_index()

        # Group temperature data by TN_Match (MECS_NAICS) and recalculate
        # energy fraction by temperature.
#        eu_temp = updated_temps.groupby(['TN_Match', 'Heat_type', 'Temp_C'])[
#                'E_Btu'
#                ].sum()
#
#        eu_temp = pd.concat(
#                [eu_temp, eu_temp.divide(
#                        eu_temp.sum(level=(0,1)).reindex(index=eu_temp.index)
#                        )], axis=1
#            )
#
#        eu_temp.columns = ['E_Btu', 'fraction']
#
#        eu_temp.index.names = ['MECS_NAICS', 'Heat_type',  'Temp_C']

#        eu_temp.reset_index('Temp_C', inplace=True)
#
#        eu_temp.index.names = ['MECS_NAICS', 'Heat_type']
#
#        eu_temp['Fraction'] = eu_temp.E_Btu.divide(
#                eu_temp.sum(level=[0,1]).E_Btu
#                )

        def map_eu(mecs_eu):

            if mecs_eu in self.eu_map.keys():

                temp_use = self.eu_map[mecs_eu]

            else:

                temp_use = np.nan

            return temp_use

        eu_energy_temp_dd['Heat_type'] = eu_energy_temp_dd.End_use.apply(
                lambda x: map_eu(x), meta=('Heat_type', str)
                )

        # Drop all enduses not related to heat
        eu_energy_temp_dd = eu_energy_temp_dd.dropna(subset=['Heat_type'])

        # Work-around for index name disappearing somwhere along the way.
        def f(df, name):
            df.index.name=name
            return df
        eu_energy_temp_dd.map_partitions(f, 'MECS_NAICS')

        eu_energy_temp_dd = eu_energy_temp_dd.compute()

        # Before merging, need to reindex so that heat types that have
        # multiple temperatures are included.

        eu_energy_temp_dd = dd.merge(
                eu_energy_temp_dd,
                tn_match_fraction[['naics', 'Heat_type', 'Temp_C',
                                   'Fraction']],
                on=['naics', 'Heat_type'], how='left'
                )


#        eu_energy_temp_dd = dd.merge(
#                eu_energy_temp_dd,
#                eu_temp.reset_index(['Heat_type', 'Temp_C'])[
#                        ['Heat_type', 'Temp_C', 'fraction']
#                        ],
#                on=['MECS_NAICS', 'Heat_type'], how='left'
#                )

        eu_energy_temp_dd['MMBtu_Temp'] = \
            eu_energy_temp_dd.Fraction.mul(eu_energy_temp_dd.MMBtu,
                                           fill_value=0)

        # Drop MMBtu_Temp values == 0
        eu_energy_temp_dd = eu_energy_temp_dd.mask(
                eu_energy_temp_dd.MMBtu_Temp == 0
                )

        eu_energy_temp_dd = eu_energy_temp_dd.dropna(subset=['MMBtu_Temp'])

#        self.temps.rename(columns={'TN_Match': 'MECS_NAICS'},
#                          inplace=True)
#
#        eu_energy_temp_dd = dd.merge(
#                eu_energy_temp_dd, self.temps.set_index('MECS_NAICS')[
#                        ['Temp_C', 'Temp_Bucket']
#                        ], on=['MECS_NAICS','Temp_C']
#                )

        return eu_energy_temp_dd
