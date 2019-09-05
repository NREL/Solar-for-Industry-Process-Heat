
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
import urllib
from bs4 import BeautifulSoup
import dask.dataframe as dd
from datetime import datetime as dt

class EPA_AMD:

    def __init__(self):

        self.ftp_url = "ftp://newftp.epa.gov/DMDnLoad/emissions/hourly/monthly/"

        # Build list of state abbreviations
        ssoup = BeautifulSoup(
            requests.get("https://www.50states.com/abbreviations.htm").content,
            'lxml'
            )

        self.states = \
            [ssoup.find_all('td')[x].string.lower() for x in range(1,101,2)]

        self.months = ['01','02','03','04','05','06','07','08','09','10','11',
                       '12']

        # Info from EPA on relevant facilities from
        self.am_facs = pd.read_csv(
            '../calculation_data/EPA_airmarkets_facilities.csv'
            )

        self.am_facs.columns = [x.strip() for x in self.am_facs.columns]

        self.am_facs.rename(columns={'Facility ID (ORISPL)': 'ORISPL_CODE'},
                            inplace=True)

        #Downloaded data saved as parquet files
        self.amd_files = ['epa_amd','epa_amd_final']

    def dl_data(self, years=range(2012, 2019)):
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

        all_the_data.to_parquet('../calculation_data/amd_data_2/',
                                engine='pyarrow', compression='gzip')

        print('ftp download complete')

    def format_amd(data):
        """

        """

        #Read parquet files
        amd = pd.concat(
            [pd.read_parquet(f, engine='pyarrow') for f in amd_files],
            axis=0, ignore_index=True
            )

        #Create dask dataframe with ORISPL_CODE as index
        amd_dd = dd.from_pandas(
            amd.set_index('ORISPL_CODE'),
            npartitions=len(amd.ORISPL_CODE.unique()), sort=True, name='amd'
            )

        def format_amd_dt(dt_row):

            date = dt.strptime(dt_row['OP_DATE'], '%d-%M-%Y').date()

            time = dt.strptime(str(dt_row['OP_HOUR']), '%H').time()

            tstamp = dt.combine(date, time)

            return tstamp


        amd_dd.assign(
            timestamp=amd_dd.apply(lambda x: format_amd_dt(x), axis=1,
                                   meta=('timestamp', 'datetime64'))
            )

        # Merge in info on unit types
        amd_dd =amd_dd.merge(
            self.am_facs.set_index['ORISPL_CODE'][
                ['Unit ID', 'Unit Type', 'Fuel Type (Primary)',
                'Fuel Type (Secondary)']
                ].drop_duplicates(), on=['ORISPL_CODE'], how='left'
            )

        #pd.to_datetime(all_the_data_final.loc[0, 'OP_DATE']+ ' ' + str(all_the_data_final.loc[0, 'OP_HOUR']), dayfirst=True, format="%d-%m-%Y %H")
