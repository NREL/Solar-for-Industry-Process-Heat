
import requests
import pandas as pd
import numpy as np
from zipfile import ZipFile
from io import BytesIO
import urllib
from bs4 import BeautifulSoup
import dask.dataframe as dd
import datetime as dt
import scipy.cluster as spc
import matplotlib.pyplot as plt
from pandas.tseries.holiday import USFederalHolidayCalendar
import re

class EPA_AMD:

    def __init__(self):

        self.ftp_url = "ftp://newftp.epa.gov/DMDnLoad/emissions/hourly/monthly/"

        self.months = ['01','02','03','04','05','06','07','08','09','10','11',
                       '12']

        # Info from EPA on relevant facilities from
        self.am_facs = pd.read_csv(
            '../calculation_data/EPA_airmarkets_facilities.csv'
            )

        self.am_facs.columns = [x.strip() for x in self.am_facs.columns]

        self.am_facs.rename(columns={'Facility ID (ORISPL)': 'ORISPL_CODE',
                                     'Unit ID': 'UNITID'},
                            inplace=True)

        # Fill in missing max heat rate data
        self.am_facs.set_index(['ORISPL_CODE', 'UNITID'], inplace=True)

        self.am_facs.update(
            self.am_facs['Max Hourly HI Rate (MMBtu/hr)'].fillna(method='bfill')
            )

        self.am_facs.reset_index(inplace=True)

        def unit_str_cleaner(am_facs):
            """
            Removes start date from unit type.
            """

            def search_starts(unit_type):

                try:

                    unit_start = re.search('(\(.+)', unit_type).groups()[0]

                    unit_type = unit_type.split(' '+unit_start)[0]

                except:

                    unit_start=np.nan

                return np.array([unit_type, unit_start])

            units_corrected = pd.DataFrame(
                np.vstack([x for x in am_facs['Unit Type'].apply(
                    lambda x: search_starts(x)
                    )]), columns=['Unit Type', 'Unit Start Date']
                )

            am_facs['Unit Start Date'] = np.nan

            am_facs.update(units_corrected)

            return am_facs

        # Remove starting dates from Unit type
        self.am_facs = unit_str_cleaner(self.am_facs)

        # Build list of state abbreviations
        ssoup = BeautifulSoup(
            requests.get("https://www.50states.com/abbreviations.htm").content,
            'lxml'
            )

        self.states = \
            [ssoup.find_all('td')[x].string.lower() for x in range(1,101,2)]

        fac_states = [x.lower() for x in self.am_facs.State.unique()]

        self.states = list(set.intersection(set(self.states), set(fac_states)))

        #Downloaded data saved as parquet files
        self.amd_files = \
            ['../calculation_data/'+f for f in ['epa_amd_2012-2014',
                                               'epa_amd_2015-2017',
                                               'epa_amd_2018-2019']]
        # #(partitioned by ORISPL_CODE)
        # self.amd_files = '../calculation_data/epa_amd_data'

    def dl_data(self, years=None, file_name=None):
        """
        Download and format hourly load data for specified range of years.
        """

        all_the_data = pd.DataFrame()

        for y in years:

            for state in self.states:

                for month in self.months:
            #source_address is a 2-tuple (host, port) for the socket to bind to as its source address before connecting
                    y_ftp_url = self.ftp_url+'{!s}/{!s}{!s}{!s}.zip'.format(
                        str(y),str(y),state,month
                        )

                    print(y_ftp_url)

                    try:
                        response = urllib.request.urlopen(y_ftp_url)

                    except urllib.error.URLError as e:
                        print(y, state, e)
                        continue

                    # ftp_file = response.read()

                    zfile = ZipFile(BytesIO(response.read()))

                    hourly_data = pd.read_csv(zfile.open(zfile.namelist()[0]),
                                              low_memory=False)

                    hourly_data = hourly_data[
                        hourly_data['ORISPL_CODE'].isin(
                            self.am_facs['ORISPL_CODE']
                            )
                        ]

                    if 'HEAT_INPUT' in hourly_data.columns:

                        hourly_data.dropna(subset=['HEAT_INPUT'],
                                           inplace=True)

                    if 'HEAT_INPUT (mmBtu)' in hourly_data.columns:

                        hourly_data.dropna(subset=['HEAT_INPUT (mmBtu)'],
                                           inplace=True)

                    usecols=['STATE','FACILITY_NAME','ORISPL_CODE','UNITID',
                             'OP_DATE','OP_HOUR','OP_TIME','GLOAD (MW)',
                             'SLOAD (1000lb/hr)', 'GLOAD', 'SLOAD',
                             'HEAT_INPUT','HEAT_INPUT (mmBtu)','FAC_ID',
                             'UNIT_ID','SLOAD (1000 lbs)']

                    drop_cols = set.difference(set(hourly_data.columns),usecols)

                    hourly_data.drop(drop_cols, axis=1, inplace=True)

                    all_the_data = all_the_data.append(
                        hourly_data, ignore_index=True
                        )

        all_the_data.to_parquet('../calculation_data/'+file_name,
                                engine='pyarrow', compression='gzip')

        print('ftp download complete')

    def format_amd(self, data):
        """
        Read AMD parquet files and format date and time, and add
        facility information.
        """

        def describe_date(amd_data):
            """
            Add columns for weekday, month, and holiday. Based on existing
            timestamp column in amd_data dataframe.
            """

            holidays = USFederalHolidayCalendar().holidays()

            if type(amd_data) ==  dd.DataFrame:

                amd_data = amd_data.assign(weekday=amd_data.timestamp.apply(
                    lambda x: x.weekday() > 4, meta=('weekday', 'bool')
                    ))

                amd_data = amd_data.assign(month=amd_data.timestamp.apply(
                    lambda x: x.month, meta=('month', 'int')
                    ))

                amd_data = amd_data.assign(holiday=amd_data.timestamp.apply(
                    lambda x: x.date() in holidays, meta=('holiday', 'bool')
                    ))

            if type(amd_data) == pd.DataFrame:

                amd_data['weekday'] = amd_data.timestamp.apply(
                    lambda x: x.weekday() > 4
                    )

                amd_data['month'] = amd_data.timestamp.apply(
                    lambda x: x.month
                    )

                amd_data['holiday'] = amd_data.timestamp.apply(
                    lambda x: x.date() in holidays
                    )

            return amd_data

        #Read parquet files into dask dataframe
        # Unable to use partitioned parquet because index is read
        # as an object, which precludes merging operations.
        # amd_dd = dd.read_parquet(amd_files, engine='pyarrow')
        #
        # # method for renaming index of amd_dd
        # def p_rename(df, name):
        #     df.index.name = name
        #     return df
        #
        # amd_dd.map_partitions(p_rename, 'ORISPL_CODE')
        amd_dd = dd.from_pandas(
            pd.concat(
                [pd.read_parquet(f, engine='pyarrow') for f in amd_files],
                axis=0, ignore_index=True
                ).set_index('ORISPL_CODE'), npartitions=311, sort=True,
            name='amd'
            )

        # Merge in info on unit types
        am_facs_dd_merge = self.am_facs.drop_duplicates(
            ['ORISPL_CODE', 'UNITID']
            )[
                ['ORISPL_CODE','UNITID', 'Unit Type', 'Fuel Type (Primary)',
                'Fuel Type (Secondary)', 'Max Hourly HI Rate (MMBtu/hr)']
            ].set_index(['ORISPL_CODE'])

        amd_dd = amd_dd.merge(
            am_facs_dd_merge, how='left', on=['ORISPL_CODE', 'UNITID']
            )

        def format_amd_dt(dt_row):

            date = dt.datetime.strptime(dt_row['OP_DATE'], '%m-%d-%Y').date()

            time = dt.datetime.strptime(str(dt_row['OP_HOUR']), '%H').time()

            tstamp = dt.datetime.combine(date, time)

            return tstamp

        amd_dd = amd_dd.rename(
            columns={'GLOAD':'GLOAD_MW', 'GLOAD (MW)': 'GLOAD_MW',
                     'SLOAD': 'SLOAD_1000lb_hr',
                     'SLOAD (1000lb/hr)': 'SLOAD_1000lb_hr',
                     'HEAT_INPUT': 'HEAT_INPUT_MMBtu',
                     'HEAT_INPUT (mmBtu)': 'HEAT_INPUT_MMBtu'}
            )


        # Match ORISPL to its NAICS using GHGRP data.
        xwalk_df = pd.read_excel(
            "https://www.epa.gov/sites/production/files/2015-10/" +\
            "oris-ghgrp_crosswalk_public_ry14_final.xls", skiprows=3
            )

        xwalk_df = xwalk_df[['GHGRP Facility ID', 'FACILITY NAME', 'ORIS CODE']]

        xwalk_df.replace({'No Match':np.nan}, inplace=True)

        xwalk_df['ORIS CODE'] = xwalk_df['ORIS CODE'].astype('float32')

        naics_facs = pd.merge(
            self.am_facs, xwalk_df, left_on='ORISPL_CODE',
            right_on='ORIS CODE', how='left'
            )

        # Manual matching for facs missing ORIS. Dictionary of ORIS: GHGRP FAC
        missing_oris = pd.read_csv('../calculation_data/ORIS_GHGRP_manual.csv')

        naics_facs.set_index('ORISPL_CODE', inplace=True)

        naics_facs.update(missing_oris.set_index('ORISPL_CODE'))

        naics_facs.reset_index(inplace=True)

        # Import ghgrp facilities and their NAICS Codes
        ghgrp_facs = pd.read_parquet(
            '../results/ghgrp_energy_20190801-2337.parquet', engine='pyarrow'
            )[['FACILITY_ID', 'PRIMARY_NAICS_CODE']].drop_duplicates()

        naics_facs = pd.merge(
            naics_facs, ghgrp_facs, left_on='GHGRP Facility ID',
            right_on='FACILITY_ID', how='left'
            )

        naics_facs.set_index('ORISPL_CODE', inplace=True)

        naics_facs.update(missing_oris.set_index('ORISPL_CODE'))

        naics_facs.reset_index(inplace=True)

        amd_dd = amd_dd.merge(
            naics_facs.drop_duplicates(
                subset=['ORISPL_CODE', 'UNITID']
                ).set_index('ORISPL_CODE')[['UNITID', 'PRIMARY_NAICS_CODE']],
            on=['ORISPL_CODE', 'UNITID']
            )

        amd_dd = amd_dd.assign(
            timestamp=amd_dd.apply(lambda x: format_amd_dt(x), axis=1,
                                   meta=('timestamp', 'datetime64[ns]'))
            )

        amd_dd = amd_dd.assign(OP_DATE=amd_dd.timestamp.apply(
            lambda x: x.date(), meta=('OP_DATE', 'datetime64[ns]')
            ))

        amd_dd = amd_dd.astype(
            {'OP_HOUR': 'float32', 'OP_TIME':'float32','GLOAD_MW': 'float32',
             'SLOAD_1000lb_hr':'float32', 'HEAT_INPUT_MMBtu': 'float32',
             'FAC_ID': 'float32', 'UNIT_ID': 'float32'}
             )

        amd_dd = describe_date(amd_dd)

        amd_dd = amd_dd.assign(
            heat_input_fraction=\
                amd_dd['HEAT_INPUT_MMBtu']/amd_dd['Max Hourly HI Rate (MMBtu/hr)']
            )

        amd_dd = amd_dd.assign(year=amd_dd.OP_DATE.apply(lambda x: x.year))

        amd_dd = amd_dd.replace(
            {'GLOAD_MW':0, 'SLOAD_1000lb_hr':0, 'HEAT_INPUT_MMBtu':0,
             'heat_input_fraction':0}, np.nan
             )

        # Do those dask tasks
        amd_dd = amd_dd.compute()

        # Calcualte hourly load as a fraction of daily heat input
        amd_dd.set_index(['UNITID', 'OP_DATE', 'timestamp'], append=True,
                         inplace=True)

        amd_dd['HI_daily_fraction'] = \
            amd_dd[['HEAT_INPUT_MMBtu']].sort_index().divide(
                amd_dd[['HEAT_INPUT_MMBtu']].resample(
                    'D', level='timestamp'
                    ).sum(), level=2
            )

        amd_dd.reset_index(inplace=True)

        return amd_dd

    def calc_rep_loadshapes(self, amd_dd, by_naics=False):
        """
        Calculate representative hourly loadshapes by facility and unit type.
        Represents hourly mean load ...
        """


        if by_naics:

            load_summary = amd_dd.groupby(
                ['PRIMARY_NAICS_CODE', 'Unit Type', 'month','holiday','weekday',
                  'OP_HOUR']
                 ).agg({'GLOAD_MW': 'mean', 'SLOAD_1000lb_hr': 'mean',
                        'HEAT_INPUT_MMBtu': 'mean'})

        else:

            load_summary = amd_dd.groupby(
                ['ORISPL_CODE', 'UNITID','month','holiday','weekday','OP_HOUR']
                ).agg(
                    {'GLOAD_MW': 'mean', 'SLOAD_1000lb_hr': 'mean',
                     'HEAT_INPUT_MMBtu': 'mean', 'heat_input_fraction':'mean'}
                     )

        for col in ['GLOAD_MW','SLOAD_1000lb_hr', 'HEAT_INPUT_MMBtu']:
            new_name = col.split('_')[0]+'_hourly_fraction'

            load_summary[new_name] = \
                load_summary[col].divide(
                    load_summary[col].sum(level=[0,1,2,3,4])
                    )

        return load_summary

    @staticmethod
    def make_load_shape_plots(load_summary, naics, unit_type, load_type):

        plot_data = load_summary.xs(
            [naics, unit_type], level=['PRIMARY_NAICS_CODE', 'Unit Type']
            )[[load_type]].reset_index()

        plot_data['holiday-weekday'] = plot_data[['holiday', 'weekday']].apply(
            lambda x: tuple(x), axis=1
            )

        grid = sns.FacetGrid(
            plot_data[[load_type, 'OP_HOUR', 'month', 'holiday-weekday']],
            col='month', hue='holiday-weekday', col_wrap=3, height=1.75)

        grid.set(ylim=(0,1))

        grid.savefig('test.png')



    @staticmethod
    def run_cluster_analysis(amd_dd, kn=range(1,30)):
        """
        Run to identify day types by unit
        """

        # pivot data so hours, weekday/weekend, holiday, and month, are columns
        # and date is row.


        for g in amd_dd.groupby(['ORISPL_CODE', 'UNIT_ID']).groups:

            data = amd_dd.groupby(
                ['ORISPL_CODE', 'UNIT_ID']
                ).get_group(g).join(
                    amd_dd.groupby(
                        ['ORISPL_CODE', 'UNIT_ID']
                        ).get_group(g).apply()
                        )

            data['TS_DATE'] = data.timestamp.apply(
                lambda x: x.date()
                )

            data = data.pivot(
                index='TS_DATE', columns='OP_HOUR',
                values='SLOAD (1000lb/hr)'
                )

            data = describe_date(data)

        def id_clusters(data):
            """
            K-means clustering hourly load by day.
            kn is the number of clusters to calculate, represented as a range
            """

            # Whiten observations (normalize by dividing each column by its standard
            # deviation across all obervations to give unit variance.
            # See scipy.cluster.vq.whiten documentation).
            # Need to whitend based on large differences in mean and variance
            # across energy use by NAICS codes.
            data_whitened = spc.vq.whiten(data)

            # Run K-means clustering for the number of clusters specified in K
            KM_load = [spc.vq.kmeans(data_whitened, k, iter=25) for k in kn]

            KM_results_dict = {}

            KM_results_dict['data_white'] = data_whitened

            KM_results_dict['KM_results'] = KM_load

            KM_results_dict['centroids'] = [cent for (cent, var) in KM_load]

            # Calculate average within-cluster sum of squares
            KM_results_dict['avgWithinSS'] = [var for (cent, var) in KM_load]

            # Plot elbow curve to examine within-cluster sum of squares
            # Displays curve and asks for input on number of clusters to use
            fig = plt.figure()

            ax = fig.add_subplot(111)

            ax.plot(kn, KM_results_dict['avgWithinSS'], 'b*-')

            #ax.plot(K[kIdx], avgWithinSS[kIdx], marker='o', markersize=12,
            #    markeredgewidth=2, markeredgecolor='r', markerfacecolor='None')
            plt.grid(True)

            plt.xlabel('Number of clusters')

            plt.ylabel('Average within-cluster sum of squares')

            plt.title('Elbow for KMeans clustering')

            plt.show(block=False)

            # User input for selecting number of clusters.
            # plt.show(block=False) or plt.pause(0.1)
            chosen_k = input("Input selected number of clusters: ")

            return chosen_k,

        def format_cluster_results(
                    KM_results_dict, cla_input, ctyfips, naics_agg, n
                    ):
            """
            Format cluster analysis results for n=k clusters, adding cluster ids
            and socio-economic data by county.
            """

            # Calcualte cluster ids and distance (distortion) between the observation
            # and its nearest code for a chosen number of clusters.
            cluster_id, distance = spc.vq.vq(
                KM_results['data_white'],
                KM_results['centroids'][chosen_k - 1]
                )

            cols = ['cluster']

            for col in data.columns:
                cols.append(col)

            # Combine cluster ids and energy data
            cluster_id.resize((cluster_id.shape[0], 1))

            # Name columns based on selected N-digit NAICS codes

            id_load_clusters = \
                pd.DataFrame(
                    np.hstack((cluster_id, data)),
                           columns=cols
                           )

            id_load_clusters.set_index(ctyfips[naics_agg], inplace=True)

            id_energy.loc[:, 'TotalEnergy'] = id_energy[cols[1:]].sum(axis=1)
