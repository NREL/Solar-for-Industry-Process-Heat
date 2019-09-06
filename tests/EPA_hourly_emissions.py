
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
import urllib
from bs4 import BeautifulSoup
import dask.dataframe as dd
import datetime as dt

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

        self.am_facs.rename(columns={'Facility ID (ORISPL)': 'ORISPL_CODE'},
                            inplace=True)

        # Build list of state abbreviations
        ssoup = BeautifulSoup(
            requests.get("https://www.50states.com/abbreviations.htm").content,
            'lxml'
            )

        self.states = \
            [ssoup.find_all('td')[x].string.lower() for x in range(1,101,2)]

        fac_states = [x.lower() for x in self.am_facs.State.unique()]

        self.states = list(set.intersection(self.states), set(fac_states))

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

    def format_amd(data):
        """

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

        def format_amd_dt(dt_row):

            date = dt.datetime.strptime(dt_row['OP_DATE'], '%d-%M-%Y').date()

            time = dt.datetime.strptime(str(dt_row['OP_HOUR']), '%H').time()

            tstamp = dt.datetime.combine(date, time)

            return tstamp


        amd_dd.assign(
            timestamp=amd_dd.apply(lambda x: format_amd_dt(x), axis=1,
                                   meta=('timestamp', 'datetime64'))
            )

        # Merge in info on unit types
        amd_dd =amd_dd.merge(
            self.am_facs.set_index(['ORISPL_CODE'])[
                ['Unit ID', 'Unit Type', 'Fuel Type (Primary)',
                'Fuel Type (Secondary)', 'Max Hourly HI Rate (MMBtu/hr)']
                ].drop_duplicates(), on=['ORISPL_CODE'], how='left'
            )


    def xwalk_NAICS_ORISPL(self, orispl_df):
        """
        Match ORISPL to its NAICS using GHGRP data.
        """

        xwalk_df = pd.read_excel(
            "https://www.epa.gov/sites/production/files/2015-10/" +\
            "oris-ghgrp_crosswalk_public_ry14_final.xls", skiprows=3
            )

        # Keep only relevant ORISPL
        # merge with self.amd_facs

        # Merge with GHGRP facilities
        # pull in all fac_table_[y].csv in calcualtion_data/ghgrp_data/

        # Need to keep track of which ORISPL aren't matched
