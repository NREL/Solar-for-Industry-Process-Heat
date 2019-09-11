import pandas as pd
import numpy as np
import datetime as dt
import dask.dataframe as dd
from pandas.tseries.holiday import USFederalHolidayCalendar


class amd_data:

    def __init__(self):

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

        self.xwalk_df = pd.read_excel(
          "https://www.epa.gov/sites/production/files/2015-10/" +\
          "oris-ghgrp_crosswalk_public_ry14_final.xls", skiprows=3
          )

        self.xwalk_df = self.xwalk_df[
            ['GHGRP Facility ID', 'FACILITY NAME', 'ORIS CODE']
            ]

        self.xwalk_df.replace({'No Match':np.nan}, inplace=True)

        self.xwalk_df['ORIS CODE'] = self.xwalk_df['ORIS CODE'].astype('float32')

        naics_facs = pd.merge(
          self.am_facs, self.xwalk_df, left_on='ORISPL_CODE',
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

        self.naics_facs = pd.merge(
          naics_facs, ghgrp_facs, left_on='GHGRP Facility ID',
          right_on='FACILITY_ID', how='left'
          )

        self.naics_facs.set_index('ORISPL_CODE', inplace=True)

        self.naics_facs.update(missing_oris.set_index('ORISPL_CODE'))

        self.naics_facs.reset_index(inplace=True)

    def format_amd_data(self, parquet_file):

        def describe_date(amd_data):
            """
            Add columns for weekday, month, and holiday. Based on existing timestamp
            column in amd_data dataframe.
            """

            holidays = USFederalHolidayCalendar().holidays()

            if type(amd_data) ==  dd.DataFrame:

                amd_data = amd_data.assign(weekday=amd_data.timestamp.map(
                    lambda x: x.weekday() > 4, meta=('weekday', 'bool')
                    ))

                amd_data = amd_data.assign(month=amd_data.timestamp.map(
                    lambda x: x.month, meta=('month', 'int')
                    ))

                amd_data = amd_data.assign(holiday=amd_data.timestamp.map(
                    lambda x: x.date() in holidays, meta=('holiday', 'bool')
                    ))

            if type(amd_data) == pd.DataFrame:

                amd_data['weekday'] = amd_data.timestamp.map(
                    lambda x: x.weekday() > 4
                    )

                amd_data['month'] = amd_data.timestamp.map(
                    lambda x: x.month
                    )

                amd_data['holiday'] = amd_data.timestamp.map(
                    lambda x: x in holidays
                    )

            return amd_data

        oris = int(parquet_file.split('=')[1])

        parquet_file = parquet_file+'/'

        amd = pd.read_parquet(parquet_file, engine='pyarrow')

        amd['ORISPL_CODE'] = oris

        amd.set_index(['ORISPL_CODE', 'UNITID'], inplace=True)

        am_facs_merge = self.am_facs.drop_duplicates(
          ['ORISPL_CODE', 'UNITID']
          )[
              ['ORISPL_CODE','UNITID', 'Unit Type', 'Fuel Type (Primary)',
              'Fuel Type (Secondary)', 'Max Hourly HI Rate (MMBtu/hr)']
          ].set_index(['ORISPL_CODE', 'UNITID'])

        amd = amd.merge(
          am_facs_merge, how='left', left_index=True, right_index=True
          )

        def format_amd_dt(dt_row):

          date = dt.datetime.strptime(dt_row['OP_DATE'], '%m-%d-%Y').date()

          time = dt.datetime.strptime(str(dt_row['OP_HOUR']), '%H').time()

          tstamp = dt.datetime.combine(date, time)

          return tstamp

        amd = amd.rename(
          columns={'GLOAD':'GLOAD_MW', 'GLOAD (MW)': 'GLOAD_MW',
                   'SLOAD': 'SLOAD_1000lb_hr',
                   'SLOAD (1000lb/hr)': 'SLOAD_1000lb_hr',
                   'HEAT_INPUT': 'HEAT_INPUT_MMBtu',
                   'HEAT_INPUT (mmBtu)': 'HEAT_INPUT_MMBtu'}
          )

        amd = amd.join(
          self.naics_facs.drop_duplicates(
              subset=['ORISPL_CODE', 'UNITID']
              ).set_index(['ORISPL_CODE', 'UNITID'])[['PRIMARY_NAICS_CODE']]
          )

        amd['timestamp'] = amd.apply(lambda x: format_amd_dt(x), axis=1)

        amd['OP_DATE'] = amd.timestamp.apply(lambda x: x.date())

        amd = amd.astype(
            {'OP_HOUR': 'float32', 'OP_TIME':'float32','GLOAD_MW': 'float32',
             'SLOAD_1000lb_hr':'float32', 'HEAT_INPUT_MMBtu': 'float32',
             'FAC_ID': 'float32', 'UNIT_ID': 'float32'}
           )

        amd = describe_date(amd)

        amd['heat_input_fraction'] = amd['HEAT_INPUT_MMBtu'].divide(
            amd['Max Hourly HI Rate (MMBtu/hr)']
            )

        return amd

    @staticmethod
    def summarize_data(amd):

        amd = amd.replace(
            {'GLOAD_MW':0, 'SLOAD_1000lb_hr':0, 'HEAT_INPUT_MMBtu':0,
             'heat_input_fraction':0}, np.nan
             )

        load_summary = amd.groupby(['month', 'holiday', 'weekday', 'OP_HOUR']).agg(
            {'GLOAD_MW': 'mean', 'SLOAD_1000lb_hr': 'mean',
             'HEAT_INPUT_MMBtu': 'mean', 'heat_input_fraction':'mean'}
            )

        return load_summary
