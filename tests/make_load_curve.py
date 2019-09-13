
import shift
import pandas as pd
import os


class load_curve:

    def __init__(self, base_year=2014):

        self.base_year = base_year
        # Input avg weekly hours by quarter, tested for seasonality.
        # Then convert to weekly hours by month.
        # Set 2014 as base year.
        self.swh = pd.read_csv(
            '../calculation_data/qpc_weekly_hours.csv'
            )

        self.swh.columns = \
            [x.upper() for x in self.swh.columns]

        self.swh = self.swh.melt(
            id_vars='NAICS', var_name='quarter', value_name='weekly_op_hours'
            )

        self.swh['quarter'] = self.swh.quarter.apply(
                lambda x: pd.Period(str(self.base_year)+x, freq='Q')
                )

        self.swh.set_index('quarter', inplace=True)

        self.swh['month'] = self.swh.index.month

        self.reset_index()

        self.swh = pd.merge(self.swh.reset_index(),
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
            '../calculation_data/iac_emp_size_scale.csv', index='naics3'
            )

        self.emp_size_adj.fillna(1, inplace=True)


        # Expand weekly hours by quarter to weekly hours by month
        # First format as time series to use pandas TS funcationality.
    def calc_operating_curve(self, naics, emp_size):
        """
        Calculate annual operating hours (8760) in 2014 by NAICS code and
        employment size class.
        """

        swh_emp_size = self.swh.copy(deep=True)

        # Scale weekly hours based on employment size class
        swh_emp_size.weekly_op_hours.update(
            swh_emp_size.weekly_op_hours*\
            self.emp_size_adj.loc[int(str(naics)[0:3]), emp_size]
            )

        
