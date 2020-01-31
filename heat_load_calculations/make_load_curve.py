
import shift
import pandas as pd
import numpy as np
import os
import classify_load_shape
from load_interpolation import interpolate_load
import compile_load_data


class load_curve:

    def __init__(self, base_year=2014):

        data_dir = './calculation_data/'

        self.base_year = base_year
        # Input avg weekly hours by quarter, tested for seasonality.
        # Then convert to weekly hours by month.
        # Set 2014 as base year.
        self.swh = pd.read_csv(
            data_dir+'qpc_weekly_hours_2014.csv', header=[0,1], index_col=0
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
            data_dir+'iac_emp_size_scale.csv', index_col='naics3'
            )

        self.emp_size_adj.fillna(1, inplace=True)

        self.load_data = compile_load_data.LoadData()

    def match_qpc_naics(self, naics, qpc_naics):

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

        return int(naics)


    def calc_load_shape(self, naics, emp_size, hours='qpc', energy='heat'):
        """
        Calculate hourly load shape (represented as a fraction of daily
        energy use) by month and day of week (Monday==0) based on
        NAICS code and employment size class.

        The default value for hours is 'qpc', the values reported by the Census
        Quarterly Survey of Plant Capacity Utilization. Otherwise, a list of
        weekly average production hours by quarter should be specified,
        e.g., [40, 40, 42, 40].
        """

        # Keep track of original NAICS code
        naics_og = naics

        # Match input naics to closest NAICS from QPC
        qpc_naics = self.swh.NAICS.unique().astype(str)

        naics = self.match_qpc_naics(naics, qpc_naics)

        # If no matching NAICS use average for all manufacturing sector
        if np.isnan(naics):

            naics == '31-33'

        swh_emp_size = pd.DataFrame(self.swh[self.swh.NAICS == naics])

        swh_emp_size.reset_index(inplace=True, drop=True)

        try:
            # Scale weekly hours based on employment size class
            # Not all 3-digit NAICS are represented, though.
            swh_emp_size.update(
                swh_emp_size[
                    ['Weekly_op_hours', 'Weekly_op_hours_low',
                     'Weekly_op_hours_high']
                    ].multiply(
                        self.emp_size_adj.loc[int(str(naics)[0:3]), emp_size]
                        )
                )

        except KeyError:

            pass

        # Use default production hour values, unless specified otherwise.
        if hours != 'qpc':
            # capture standard error of high and low estimates for weekly hours
            se = swh_emp_size[
                ['Weekly_op_hours_high','Weekly_op_hours_low']
                ].divide(swh_emp_size['Weekly_op_hours'], axis=0)

            swh_emp_size.loc[:, 'Weekly_op_hours'] = np.repeat(hours, 3)

            swh_emp_size.update(
                se.multiply(swh_emp_size.Weekly_op_hours, axis=0)
                )

        op_schedule = pd.DataFrame()

        for col in ['Weekly_op_hours', 'Weekly_op_hours_low',
                    'Weekly_op_hours_high']:

            weeksched = pd.concat([swh_emp_size[col].apply(
                lambda x: shift.schedule(
                    weekly_op_hours=x
                    ).calc_weekly_schedule()[0]
                )], axis=0, ignore_index=True)

            weeksched = pd.concat([weeksched,
                pd.concat([swh_emp_size[col].apply(
                    lambda x: shift.schedule(
                        weekly_op_hours=x
                        ).calc_weekly_schedule()[1]
                    )], axis=0, ignore_index=True)], axis=1
                )

            weeksched = pd.concat([weeksched,
                pd.concat([swh_emp_size[col].apply(
                    lambda x: shift.schedule(
                        weekly_op_hours=x
                        ).calc_weekly_schedule()[2]
                    )], axis=0, ignore_index=True)], axis=1
                )

            if col.split('_')[-1] == 'hours':

                type_col = 'schedule_type_mean'

            else:

                type_col = 'schedule_type_'+col.split('_')[-1]

            weeksched.columns = [col, type_col, 'daily_hours']

            months = pd.DataFrame(
                [x+1 for x in weeksched.index], index=weeksched.index,
                columns=['month']
                )

            weeksched = weeksched.join(months)

            for x in weeksched.index:

                unpacked = weeksched.loc[x][0].copy()

                unpacked[type_col] = weeksched.loc[x][type_col]

                unpacked['month'] = weeksched.loc[x]['month']

                unpacked = pd.merge(
                    unpacked, weeksched.loc[x]['daily_hours'], right_index=True,
                    left_on='dayofweek'
                    )

                unpacked.rename(columns={'operating': col}, inplace=True)

                unpacked.reset_index(inplace=True, drop=True)

                unpacked.set_index(['month', 'dayofweek', 'hour'],
                                   inplace=True)

                op_schedule = op_schedule.append(unpacked)

        op_schedule['NAICS'] = naics

        op_schedule['emp_size'] = emp_size

        # Operating schedule includes low, mean, and high operating
        # hour estimates.
        op_hours = ['Weekly_op_hours', 'Weekly_op_hours_low',
                    'Weekly_op_hours_high']

        load_shape = pd.DataFrame(index=op_schedule.index,columns=op_hours)


        # Estimate peak and minimum loads based on NAICS and employment
        # size class
        load_factor, min_peak_loads = \
            self.load_data.select_load_data(naics_og, emp_size)

        # Perform only once per quarter.
        for m in range(1, 12, 3):

            for hours_type in op_hours:

                sched = op_schedule[hours_type].dropna()

                for day in [0, 5, 6]:







                # Need to treat the minimum of EPA-based load shapes differently than
                # EPRI-based. This is because EPA data are for heat demand and EPRI
                # data are for electricity demand.
                if 'epa' != min_peak_loads.source.unique()[0]:

                    #
                    turndown =

                    # Create final load shape by interpolating between min and peak loads
                    # using operating schedule.
                    # Interpolates for a single day type.
                    op_schedule = interpolate_load(op_schedule, peak_load,
                                                   turndown=turndown)




        return op_schedule, load_factor, load_shape, min_peak_loads

        # Final step is to multiply by monthly load factor
        load_shape = load_shape[load_shape.columns].multiply(
            load_factor.load_factor.set_index('month', inplace=True), axis=0,
            level=0
            )

        return op_schedule

    @staticmethod
    def calc_annual_load(op_schedule, annual_energy_use):
        """
        Calculate hourly heat load based on operating schedule and
        annual energy use (in MMBtu) for 2014.
        Returns hourly load for mean, high, and low weekly hours by quarter
        (high and low are 95% confidence interval).
        """

        # Make dataframe for 2014 in hourly timesteps
        dtindex = pd.date_range('2014-01-01', periods=8760, freq='H')

        load_8760 = pd.DataFrame(index=dtindex)

        load_8760['month'] = load_8760.index.month

        load_8760['dayofweek'] = load_8760.index.dayofweek

        load_8760['Q'] = load_8760.index.quarter

        load_8760['hour'] = load_8760.index.hour

        load_8760['date'] = load_8760.index.date

        # Calculate total number of days by
        # Used to scale load shapes
        day_count = load_8760.drop_duplicates('date').groupby(
            ['month', 'dayofweek']
            ).date.count()

        op_schedule = op_schedule.reset_index().melt(
            id_vars=['month', 'dayofweek', 'hour', 'NAICS', 'emp_size',
                     'fraction_annual_energy', 'daily_hours'],
            var_name='hours_type', value_name='schedule_type'
            ).dropna()

        op_schedule.hours_type.replace(
            {'schedule_type_mean': 'mean', 'schedule_type_high': 'high',
             'schedule_type_low': 'low'}, inplace=True
            )

        op_schedule.set_index(
            ['hours_type', 'month', 'dayofweek', 'hour'], inplace=True
            )

        # Need to also sum daily hours by quarter and use fraction of annual
        # fraction to split annual energy into quarterly energy.
        op_schedule.fraction_annual_energy.update(
            op_schedule.fraction_annual_energy.divide(day_count)
            )

        load_8760 = pd.merge(load_8760.reset_index(), op_schedule.reset_index(),
                             on=['month', 'dayofweek','hour'])

        q_energy_fraction = load_8760.groupby(['hours_type', 'Q']).apply(
            lambda x: x.drop_duplicates('date').daily_hours.sum()
            )

        q_energy_fraction.update(
            q_energy_fraction.divide(q_energy_fraction.sum(level=0))
            )

        q_energy = q_energy_fraction.multiply(annual_energy_use)

        load_8760.sort_values(by=['date', 'hour'], inplace=True)

        load_8760.index = dtindex

    def calc_8760_load(self, annual_mmbtu, op_schedule):
        """
        Calculate hourly heat load data based on operating schedule, etc.
        """
        # How is QPC NAICS matched?
        # Need to determine shifts by day type in op_schedule
        # if n_shifts < 3, use epri load shape info from hours 0 - 14:00

        # For interpolating

        emp_size = op_schedule['emp_size'].unique()

        naics = op_schedule['NAICS'].unique()

        large_sizes = ['n250_499', 'n500_999', 'n1000', 'ghgrp']

        if emp_size in large_sizes:

            lf = epa_lf

            if naics not in epa_lf:

              lf = epri_lf
