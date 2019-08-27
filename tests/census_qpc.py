import requests
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller
from scipy.signal import detrend
from statsmodels.formula.api import ols

class QPC:

    def get_qpc_data(year):
        """
        Quarterly survey began 2008; start with 2010 due to  2007-2009
        recession.
        """
        y = str(year)

        if year < 2017:

            excel_ex = '.xls?#'

        else:

            excel_ex = '.xlsx?#'

        qpc_data = pd.DataFrame()

        base_url = 'https://www2.census.gov/programs-surveys/qpc/tables/'

        for q in ['q'+str(n) for n in range(1, 5)]:

            if year >= 2017:

                y_url = '{!s}/{!s}_qtr_table_final_'

            # elif year < 2010:
            #
            #     y_url = \
            #         '{!s}/qpc-quarterly-tables/{!s}_qtr_combined_tables_final_'

            else:

                y_url = '{!s}/qpc-quarterly-tables/{!s}_qtr_table_final_'

            if (year == 2016) & (q == 'q4'):

                url = base_url + y_url.format(y, y) + q + '.xlsx?#'

            else:

                url = base_url + y_url.format(y, y) + q + excel_ex

            print(url)

            #Excel formatting for 2008 is different than all other years.
            #Will need to revise skiprows and usecols.
            data = pd.read_excel(url, sheet_name=1, skiprows=4, usecols=6,
                                 header=0)

            data.drop(data.columns[2], axis=1, inplace=True)

            data.columns = ['NAICS', 'Description', 'Utilization Rate',
                            'UR_Standard Error',
                            'Weekly_op_hours',
                            'Hours_Standard Error']

            data.dropna(inplace=True)

            data['Q'] = q

            data['Year'] = year

            qpc_data = qpc_data.append(data, ignore_index=True)

        # Some NAICS aren't converting from string to int using .astype
        # e.g., '31519'
        def force_format(naics):

            try:
                naics = int(naics)

                return naics

            except ValueError:

                return naics

        def format_naics(df):
            """
            Formats results that aggregate NAICS codes (e.g., "3113, 4")
            """

            for naics in df.NAICS.unique():

                all_naics = []

                if naics == '31-33':

                    continue

                if type(naics) != str:

                    continue

                elif ',' in naics:

                    all_naics.append(int(naics.split(',')[0]))

                    for n in naics.split(',')[1:]:

                        n = n.strip()

                        if '-' in n:

                            for m in range(
                                int(naics.split('-')[0][-1])+1,
                                    int(naics.split('-')[1])+1
                                ):

                                all_naics.append(
                                    int(naics.split(',')[0][:-1]+str(m))
                                    )

                        else:

                            all_naics.append(
                                int(naics.split(',')[0][:-1]+n)
                                )

                elif (',' not in naics) & ('-' in naics):

                    all_naics.append(int(naics.split('-')[0]))

                    for m in range(
                        int(naics.split('-')[0][-1])+1,
                            int(naics.split('-')[1])+1
                        ):

                        all_naics.append(
                            int(naics.split('-')[0][:-1]+str(m))
                            )

                new_rows = pd.DataFrame(
                    np.tile(df[df.NAICS == naics].values,
                    (len(all_naics), 1)), columns=df.columns
                    )

                new_rows['NAICS'] = np.repeat(all_naics,
                                              len(new_rows)/len(all_naics))

                # Delete original data
                df = df[df.NAICS != naics]

                df = df.append(new_rows, ignore_index=True)

            return df

        qpc_data.NAICS.update(
            qpc_data.NAICS.apply(lambda x: force_format(x))
            )

        qpc_data = format_naics(qpc_data)

        # Drop withheld estimates
        qpc_data = qpc_data[qpc_data.Weekly_op_hours != 'D']

        #Interpolate for single value == 'S'
        qpc_data.replace('S', np.nan, inplace=True)

        qpc_data.Weekly_op_hours.update(
            qpc_data.Weekly_op_hours.interpolate()
            )

        qpc_data['Hours_Standard Error'].update(
            qpc_data['Hours_Standard Error'].interpolate()
            )

        qpc_data['Weekly_op_hours'] = \
            qpc_data.Weekly_op_hours.astype(np.float32)

        return qpc_data

    def test_seasonality(qpc_data_naics):
        """
        Test for seasonality between quarters by NAICS using OLS.

        """

        #sort data
        qpc_data_naics.sort_values(by=['Year', 'Q'], ascending=True,
                                   inplace=True)

        # Test if data are trend stationary.
        # Null hypothesis is that there is a unit root (nonstationary)
        # Returns (test_stat, pvalue, usedlag, nobs, critical_values, icbest,
        # resstore)
        adf_test = adfuller(
            qpc_data_naics['Weekly_op_hours'].values, regression='ct',
            )

        # Can't reject null if p-value is > critical value.
        # Remove trend for seasonality testing
        if adf_test[1] > 0.05:

            qpc_data_naics['hours_detrended'] = detrend(
                qpc_data_naics['Weekly_op_hours'], type='linear'
                )

            # Constant is q1 season.
            ols_season = ols('hours_detrended ~ C(Q)', data=qpc_data_naics).fit()

        else:

            ols_season = \
                ols('Weekly_op_hours ~ C(Q)', data=qpc_data_naics).fit()

        ols_final = pd.DataFrame(np.nan, columns=['q1', 'q2', 'q3', 'q4'],
                                 index=[qpc_data_naics.NAICS.unique()[0]])

        if any(ols_season.pvalues<0.05):

            ols_res = pd.DataFrame(
                np.multiply(ols_season.pvalues<0.05, ols_season.params),
                )

            ols_res = ols_res[ols_res>0].T

            ols_res.rename(
                columns={'Intercept': 'q1', 'C(Q)[T.q2]': 'q2',
                   'C(Q)[T.q2]': 'q2',
                   'C(Q)[T.q3]': 'q3',
                   'C(Q)[T.q4]': 'q4'},
                index={0:qpc_data_naics.NAICS.unique()[0]}, inplace=True
                )

            ols_final.update(ols_res)

        return ols_final

    def calc_quarterly_avgs(qpc_data, qpc_seasonality):
        """
        Calculate average weekly operating hours by NAICS from census QPC data.
        Accounts for quarterly seasonality results: NAICS without seasonality
        are average across all quarters in date range.
        """

        annual = qpc_seasonality.isnull().apply(lambda x: all(x), axis=1)

        annual = annual[annual==True]

        annual.name = 'annual'

        ann_avg = qpc_data.set_index('NAICS').join(annual, how='inner')

        ann_avg.index.name = 'NAICS'

        ann_avg = ann_avg.reset_index().groupby('NAICS').Weekly_op_hours.mean()

        ann_avg = pd.DataFrame(np.repeat(ann_avg, 4))

        ann_avg['Q'] = np.tile(['q1', 'q2', 'q3', 'q4'],
                               int(len(ann_avg)/4))

        ann_avg = ann_avg.reset_index().pivot(
            'NAICS', 'Q', 'Weekly_op_hours'
            )

        seasonal = qpc_seasonality.dropna(thresh=1).reset_index().melt(
            id_vars='index', var_name='Q', value_name='seasonality'
            )

        seasonal['seasonality'] = seasonal.seasonality.notnull()

        seasonal.rename(columns={'index': 'NAICS'}, inplace=True)

        seasonal.index.name = 'NAICS'

        seasonal.set_index('Q', append=True, inplace=True)

        seasonal = pd.merge(
            seasonal, qpc_data, on=['NAICS', 'Q'], how='inner'
            )

        seasonal_avg = seasonal.groupby(
            ['NAICS', 'seasonality']
            ).Weekly_op_hours.mean()

        seasonal_avg = seasonal_avg.reindex(index=seasonal.groupby(
            ['NAICS', 'seasonality', 'Q']
            ).Weekly_op_hours.mean().index)

        seasonal_avg.reset_index('seasonality', drop=True, inplace=True)

        seasonal_avg = seasonal_avg.reset_index().pivot(
            'NAICS', 'Q', 'Weekly_op_hours'
            )

        all_avg = pd.concat([seasonal_avg, ann_avg], axis=0, sort=True)

        return all_avg


qpc_data = pd.concat(
    [get_qpc_data(y) for y in range(2010, 2019)], axis=0, ignore_index=True
    )

qpc_seasonality = pd.concat(
    [test_seasonality(
        qpc_data[qpc_data.NAICS == naics]
        ) for naics in qpc_data.NAICS.unique()], axis=0, ignore_index=False
    )

qpc_weekly_hours = calc_quarterly_avgs(qpc_data, qpc_seasonality)

qpc_weekly_hours.to_csv('../results/qpc_weekly_hours.csv')
