
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
import urllib
from bs4 import BeautifulSoup
import dask.dataframe as dd
import datetime as dt
import scipy.cluster as spc
import matplotlib.pyplot as plt
from pandas.tseries.holiday import USFederalHolidayCalendar

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
        self.amd_files = ['epa_amd','epa_amd_final']

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

    @classmethod
    def describe_date(amd_data):
        """
        Add columns for weekday, month, and holiday. Based on existing timestamp
        column in amd_data dataframe.
        """

        fac_data['weekday'] = fac_data.timestamp.map(
            lambda x: x.weekday() > 4
            )

        fac_data['month'] = fac_data.timestamp.map(
            lambda x: x.month
            )

        holidays = USFederalHolidayCalendar().holidays()

        fac_data['holiday'] = fac_data.timestamp.map(
            lambda x: x in holidays
            )

        fac_data.replace({True:1, False:0}, inplace=True)

        fac_data.fillna(0, inplace=True)

        return fac_data


    def format_amd(self, data):
        """
        Read AMD parquet files and format date and time, and add
        facility information.
        """

        #Read parquet files
        amd = pd.concat(
            [pd.read_parquet(
                '../calculation_data/'+f, engine='pyarrow'
                ) for f in self.amd_files],
            axis=0, ignore_index=True
            )

        #Create dask dataframe with ORISPL_CODE as index
        amd_dd = dd.from_pandas(
            amd.set_index('ORISPL_CODE'),
            npartitions=len(amd.ORISPL_CODE.unique()), sort=True, name='amd'
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

        # Fill in missing max heat rates (dropped during drop_duplicates)

        # amd_dd =amd_dd.merge(
        #     self.am_facs.set_index(['ORISPL_CODE'])[
        #         ['Unit ID', 'Unit Type', 'Fuel Type (Primary)',
        #         'Fuel Type (Secondary)', 'Max Hourly HI Rate (MMBtu/hr)']
        #         ].drop_duplicates(), how='left', on=['ORISPL_CODE', 'Unit ID']
        #     )

        def format_amd_dt(dt_row):

            date = dt.datetime.strptime(dt_row['OP_DATE'], '%m-%d-%Y').date()

            time = dt.datetime.strptime(str(dt_row['OP_HOUR']), '%H').time()

            tstamp = dt.datetime.combine(date, time)

            return tstamp

        amd_dd = amd_dd.assign(
            timestamp=amd_dd.apply(lambda x: format_amd_dt(x), axis=1,
                                   meta=('timestamp', 'datetime64[ns]'))
            )

        amd_dd = amd_dd.assign(OP_DATE=amd.timestamp.apply(
            lambda x: x.date()
            ))

        # amd_dd.OP_DATE = amd_dd.timestamp.apply(
        #     lambda x: x.date(), meta={'OP_DATE': 'datetime64[ns]'}
        #     )

        amd_dd = amd_dd.rename(
            columns={'GLOAD':'GLOAD_MW', 'GLOAD (MW)': 'GLOAD_MW',
                     'SLOAD': 'SLOAD_1000lb_hr',
                     'SLOAD (1000lb/hr)': 'SLOAD_1000lb_hr',
                     'HEAT_INPUT': 'HEAT_INPUT_MMBtu',
                     'HEAT_INPUT (mmBtu)': 'HEAT_INPUT_MMBtu'}
            )

        amd_dd = amd_dd.astype(
            {'OP_HOUR': 'float32', 'OP_TIME':'float32','GLOAD_MW': 'float32',
             'SLOAD_1000lb_hr':'float32', 'HEAT_INPUT_MMBtu': 'float32',
             'FAC_ID': 'float32', 'UNIT_ID': 'float32',
             'Max Hourly HI Rate (MMBtu/hr)': 'float32'}
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
        # ghgrp_facs = pd.DataFrame()
        #
        # for y in range(2010, 2019):
        #
        #     file = '../calculation_data/ghgrp_data/fac_table_{!s}.csv'
        #
        #     try:
        #         fac_y = pd.read_csv(
        #             file.format(str(y)),
        #             usecols=['FACILITY_ID', 'PRIMARY_NAICS_CODE']
        #             )
        #
        #         ghgrp_facs = ghgrp_facs.append(fac_y, ignore_index=True)
        #
        #     except:
        #
        #         continue


        naics_facs = pd.merge(
            naics_facs, ghgrp_facs, left_on='GHGRP Facility ID',
            right_on='FACILITY_ID', how='left'
            )

        naics_facs.set_index('ORISPL_CODE', inplace=True)

        naics_facs.update(missing_oris.set_index('ORISPL_CODE'))

        amd_dd = amd_dd.merge(
            naics_facs.drop_duplicates(subset=['ORISPL_CODE', 'Unit ID']).set_index(['ORISPL_CODE', 'Unit ID']),
            on)


    def calc_rep_loadshapes(self, amd_dd):
        """
        Calculate representative hourly loadshapes by facility and unit type.
        Represents hourly mean load ...
        """
        # Calculate hourly heat input as fraction of ourly max input rate
        amd_dd.assign(
            heat_input_fraction=\
                df['HEAT_INPUT_MMBtu']/df['Max Hourly HI Rate (MMBtu/hr)''],
            meta={'heat_input_fraction':'float32'}
            )

        amd_dd.groupby(['ORISPL_CODE', 'UNIT_ID', ''])

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
