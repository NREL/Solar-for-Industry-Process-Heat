# download and unzip IAC database in memory
# map SIC to NAICS
# statistical analysis of operating hours by industry, and by year and industry,
# also employees and production level?
# See if hours have changed
# Look at summary stats for each industry
# Create distributions for each industry

from io import BytesIO
from zipfile import ZipFile
import requests
import pandas as pd
import os
from scipy import stats

class IAC:
    """
    Class for downloading, formatting, and analyzing IAC data.
    """

    def __init__(self):

        self.iac_url = 'https://iac.university/IAC_Database.zip'

        req = requests.get(self.iac_url)

        zipfile = ZipFile(BytesIO(req.content))

        self.iac = pd.read_excel(zipfile.open(zipfile.namelist()[0]),
                                 sheet_name=['ASSESS'])

        self.sic_naics = os.path.join('./', '1987_SIC_to_2002_NAICS.xls')

    def format_iac(self):
        """
        Fill in missing NAICS codes by matching SIC codes.
        """

        sic_to_naics = pd.read_csv(self.sic_naics)

        mapped_naics = pd.merge(self.iac[['SIC', 'ID']],
                                sic_to_naics[['SIC', '2002 NAICS']],
                                on=['SIC'], how='left')

        mapped_naics.rename(columns={'2002 NAICS': 'NAICS'}, inplace=True)

        self.iac.NAICS.update(mapped_naics)

        iac_grpd_ = {}

        for c in ['SIC', 'NAICS']:

            if c == 'NAICS':
                r_l, r_u = 310000, 399999

            if c == 'SIC':
                r_l, r_u = 2011, 3999

            iac_grpd[c] = self.iac[
                (self.iac.PRODHOURS.notnull()) &
                (self.iac[c].between(r_l, r_u)) &
                (self.iac.PRODHOURS.between(1000, 8760))][
                    ['FY', c, 'EMPLOYEES', 'PRODLEVEL', 'PRODUNITS']
                    ].groupby(c)

        # Summarize

    def prodhours_ols(self):
        """
        Run ordinary least squares (OLS) regression for the relationship
        between number of employees and production hours by SIC. Saves
        results as csv file.
        """

        # Only include facilities that report annual production hours between
        # 1000 and 8760 and facilities that report the number of employees.
        ols_data = self.iac[
            (self.iac.PRODHOURS.between(1000, 8760)) &
            (self.iac.EMPLOYEES.notnull())
            ][['FY', 'SIC', 'EMPLOYEES', 'PRODHOURS', 'PRODLEVEL','PRODUNITS',
               'ID']]

        # only include SICs with 10 or more observations. This leaves
        # 348 SICs, but not all are manufacturing industries.
        ols_SIC = ols_data.groupby('SIC', as_index=False).ID.count()

        ols_SIC = ols_SIC[ols_SIC.ID >= 10]

        self.ols_data = pd.merge(ols_data, ols_SIC, on='SIC', how='right')

        # Loop through SICs, performing OLS and saving results
        results_list = []

        for SIC in self.ols_data.SIC.unique():

            x = self.ols_data[self.ols_data.SIC == SIC].EMPLOYEES.values

            y = self.ols_data[self.ols_data.SIC == SIC].PRODHOURS.values

            sic_count = self.ols_data[self.ols_data.SIC == SIC].ID_y.values[0]

            slope, intercept, r_value, p_value, std_error = \
                stats.linregress(x, y)

            results_list.append(
                [SIC, sic_count, slope, intercept, r_value, p_value, std_error]
                )

        ols_results = pd.DataFrame(
            results_list, columns=['SIC', 'sic_count', 'slope', 'intercept',
                                   'r_value', 'p_value', 'std_error']
            )

        ols_results['r_squared'] = ols_results.r_value**2

        ols_results.to_csv('ols_results_emp_prodhours.csv')

    def test_normality(self, p_value=0.05):
        """
        Calculate the Shapiro-Wilk (S-W) test that production hours by SIC
        are normally distributed. S-W tests the null hypothesis that the data
        was drawn from a normal distribution.
        """

        results_list = []

        # Loop through SICs, calculating S-W test.
        for SIC in self.ols_data.SIC.unique():

            sw_stat, p_val = stats.shapiro(
                self.ols_data[self.ols_data.SIC == SIC].PRODHOURS.values
                )

            sic_count = self.ols_data[self.ols_data.SIC == SIC].ID_y.values[0]

            results_list.append([SIC, sic_count, sw_stat, p_val])

        s_w_results = pd.DataFrame(
            results_list, columns=['SIC', 'sic_count', 'S-W', 'p_value']
            )

        s_w_results.to_csv('sw_normality_prodhours.csv')

        # For results below a certain p-value, calculate normal distribution
        # parameters

        sic_dist_params = []

        for sic in sw_results[sw_results.p_value <= p_value].SIC.values:

            loc, scale = stats.norm.fit(
                self.ols_data[self.ols_data.SIC == SIC].PRODHOURS.values
                )

            sic_dist_params.append([sic, loc, scale])

        sic_dist_results = pd.DataFrame(
            sic_dist_params, columns=['SIC', 'loc_param', 'scale_param']
            )



        # What about testing via OLS if there are trends in production hours
        # over time
