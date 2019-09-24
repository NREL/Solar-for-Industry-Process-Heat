
import shift
import pandas as pd
import numpy as np
import os


class load_curve:

    def __init__(self, base_year=2014):

        self.base_year = base_year
        # Input avg weekly hours by quarter, tested for seasonality.
        # Then convert to weekly hours by month.
        # Set 2014 as base year.
        self.swh = pd.read_csv(
            '../calculation_data/qpc_weekly_hours_2014.csv', header=[0,1],
            index_col=0
            )

        self.swh.columns.set_levels(['Q1', 'Q2', 'Q3', 'Q4'], level=1,
                                    inplace=True)

        self.swh = self.swh.reset_index().melt(
            id_vars=['NAICS'], var_name=['category', 'quarter'],
            value_name='weekly_hours'
            )

        self.swh['quarter'] = self.swh.quarter.apply(
                lambda x: pd.Period(str(self.base_year)+x, freq='Q')
                )

        self.swh.set_index('quarter', inplace=True)

        self.swh['month'] = self.swh.index.month

        self.swh = self.swh.reset_index().pivot_table(values='weekly_hours',
            index=['NAICS', 'quarter', 'month'], columns='category',
            aggfunc='mean'
            ).reset_index()

        self.swh = pd.merge(self.swh,
            pd.DataFrame(np.vstack((
                np.repeat(self.swh.NAICS.unique(), 12),
                np.tile(range(1, 13),int(len(self.swh.NAICS.unique()))))).T,
                columns=['NAICS', 'month']), on=['NAICS', 'month'],
            how='outer')

        self.swh.set_index(['NAICS', 'month'], inplace=True)

        self.swh.sort_index(inplace=True)

        self.swh.fillna(method='bfill', inplace=True)

        self.swh.reset_index(inplace=True)

        # Input employment size class adjustments
        self.emp_size_adj = pd.read_csv(
            '../calculation_data/iac_emp_size_scale.csv', index_col='naics3'
            )

        self.emp_size_adj.fillna(1, inplace=True)

        @staticmethod
        def match_qpc_naics(amd_dd, qpc_data):
            """
            Match NAICS used in EPA AMD data to NAICS used in QPC survey.
            """
            amd_naics = pd.DataFrame(
                amd_dd.PRIMARY_NAICS_CODE.dropna().unique().astype(int),
                columns=['PRIMARY_NAICS_CODE']
                )

            qpc_naics = qpc_data.NAICS.astype(str).values

            def make_match(naics, qpc_naics):

                n = 6

                matched = str(naics) in qpc_naics

                while (matched == False) & (n>0):

                    n = n-1

                    naics = str(naics)[0:n]

                    matched = naics in qpc_naics

                try:

                    naics = int(naics)

                except ValueError:

                    naics = np.nan

                return naics

            amd_naics['qpc_naics'] = amd_naics.PRIMARY_NAICS_CODE.apply(
                lambda x: make_match(x, qpc_naics)
                )

            amd_dd = pd.merge(amd_dd.reset_index(), amd_naics, how='left',
                              on='PRIMARY_NAICS_CODE')

            return amd_dd

        # Import EPA AMD heat load data.
        self.amd_data = pd.read_parquet(
            '../calculation_data/epa_amd_data_formatted_20190923',
            engine='pyarrow'
            )


        # Expand weekly hours by quarter to weekly hours by month
        # First format as time series to use pandas TS funcationality.
    def calc_operating_curve(self, naics, emp_size):
        """
        Calculate annual operating hours (8760) in 2014 by NAICS code and
        employment size class.
        """

        # Make dataframe for 2014 in hourly timesteps
        dtindex = pd.date_range('2014-01-01', periods=8760, freq='H')

        load_8760 = pd.DataFrame(index=dtindex)

        load_8760['month'] = load_8760.index.month

        load_8760['dayofweek'] = load_8760.index.dayofweek

        def rename_dayofweek(dayofweek):

            if dayofweek <5:
                name='weekday'

            elif dayofweek == 5:

                name='saturday'

            else:


        load_8760['hour'] = load_8760.index.hour

        load_8760['date'] = load_8760.index.date

        # Calculate total number of days by
        # Used to scale EPA AMD load shapes
        day_count = load_8760.drop_duplicates('date').groupby(
            ['holiday', 'dayofweek']
            ).date.count()

        # Match input naics to closest NAICS from QPC
        qpc_naics = self.swh.NAICS.unique().astype(str)

        def make_match(naics, qpc_naics):

            n = 6

            matched = str(naics) in qpc_naics

            while matched == False:

                n = n-1

                try:

                    naics = str(naics)[0:n]

                    matched = naics in qpc_naics

                except IndexError:

                    return np.nan

            return int(naics)

        naics = make_match(naics, qpc_naics)

        # If no matching NAICS use average for all manufacturing sector
        if np.isnnan(naics):

            naics == '31-33'

        swh_emp_size = pd.DataFrame(self.swh[self.swh.NAICS == naics])

        swh_emp_size.reset_index(inplace=True, drop=True)

        # Scale weekly hours based on employment size class
        swh_emp_size.update(
            swh_emp_size[
                ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
                ].multiply(
                    self.emp_size_adj.loc[int(str(naics)[0:3]), emp_size]
                    )

        op_schedule = pd.DataFrame()

        for col in ['Weekly_op_hours', 'Weekly_op_hours_low',
                    'Weekly_op_hours_high']:

            blah = pd.concat([swh_emp_size[col].apply(
                lambda x: shift.schedule(weekly_op_hrs=x).calc_weekly_schedule()
                )], axis=0, ignore_index=True)

            for i in blah.index:
                blah[i]['month'] = i+1

            blah = pd.concat(
                [blah[x] for x in blah.index], axis=0, ignore_index=True
                )

            blah.rename(columns={'operating': col}, inplace=True)

            load_8760 = pd.merge(load_8760, blah,
                                 on=['month', 'dayofweek','hour'])

        load_8760.sort_values(by=['date', 'hour'], inplace=True)

        load_8760.index = dtindex
