
import shift
import pandas as pd
import os


class load_curve:

    def __init__(self):

        if 'qpc_weekly_hours' not in os.ls('../calculation_Data'):

            run census_qpc.py

        self.weekly_hours = pd.read_csv(
            '../calculation_data/qpc_weekly_hours.csv'
            )

        # Load in IAC size dependency


        # Expand weekly hours by quarter to weekly hours by month
        # First format as time series to use pandas TS funcationality.
    def calc_operating_curve(self, naics):
        """
        Calculate annual operating hours (8760) by NAICS code.
        """
