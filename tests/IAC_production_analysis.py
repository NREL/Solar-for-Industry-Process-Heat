e# download and unzip IAC database in memory
# map SIC to NAICS
# statistical analysis of operating hours by industry, and by year and industry,
# also employees and production level?
# See if hours have changed
# Look at summary stats for each industry
# Create distributions for each industry, where appropriate

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

        _url = 'https://iac.university/IAC_Database.zip'

        req = requests.get(self.iac_url)

        zipfile = ZipFile(BytesIO(req.content))

        self.iac = pd.read_excel(zipfile.open(zipfile.namelist()[0]),
                                 sheet_name=['ASSESS'])

        self.iac = pd.DataFrame(self.iac['ASSESS'])

        # Filter out annual production hours that are below 1000 and above
        # 8760
        self.iac = self.iac[self.iac.PRODHOURS.between(1000, 8760)]

        self.sic_naics = os.path.join('./', '1987_SIC_to_2002_NAICS.csv')

    def format_iac(self):
        """
        Fill in missing NAICS codes by matching SIC codes.
        """

        sic_to_naics = pd.read_csv(self.sic_naics)

        sic_to_naics.rename(columns={'2002 NAICS': 'NAICS'}, inplace=True)

        mapped_naics = pd.merge(self.iac[['SIC', 'ID']],
                                sic_to_naics[['SIC', 'NAICS']],
                                on=['SIC'], how='left')

        # Some SIC codes correspond to more than one NAICS.
        # First fill in missing NAICS for SICs with only one NAICS
        sic_single = sic_to_naics.groupby(
            'SIC', as_index=False
            )['NAICS'].count()

        sic_single = sic_single[sic_single['NAICS'] == 1]

        mapped_naics_single = pd.merge(mapped_naics, sic_single, on='SIC')

        self.iac.NAICS.update(mapped_naics_single)

        #Fill in other missing NAICS for SIC with 2-3 NAICS by aggregating
        # to 3-4 digit NAICS
        def check_naics_agg(sic_group):
            """
            Method for determining if a set of NAICS associated with a single
            SIC can be aggregated to the same 4- or 3-digit level. Will
            return nan if aggregation is not possible.
            """

            n_naics = len(sic_group.NAICS)

            naics = np.nan

            for n in range(0, n_naics-1):

                if str(sic_group.NAICS.values[n])[0:4] == \
                    str(sic_group.NAICS.values[n+1])[0:4]:

                    naics = int(str(sic_group.NAICS.values[n])[0:4])

                else:

                    if str(sic_group.NAICS.values[n])[0:3] == \
                        str(sic_group.NAICS.values[n+1])[0:3]:

                        naics = int(str(sic_group.NAICS.values[0])[0:3])

                    else:

                        naics = np.nan

                        break

            return naics

        agg_naics = []

        sicgrpd = sic_to_naics.groupby('SIC')

        for sic in sicgrpd.groups:

            agg_naics.append([sic, check_naics_agg(sicgrpd.get_group(sic))])

        agg_naics = pd.DataFrame(agg_naics, columns=['SIC', 'NAICS'])

        #Update missing NAICS based on aggregated values
        agg_update = pd.DataFrame(self.iac[self.iac.NAICS.isnull()])

        agg_update['iac_index'] = agg_update.index

        agg_update.set_index('SIC', inplace=True)

        agg_naics.set_index('SIC', inplace=True)

        agg_update.update(agg_naics)

        agg_update.reset_index(inplace=True)

        agg_update.set_index('iac_index', inplace=True)

        agg_update.index.name = 'index'

        self.iac.update(agg_update.NAICS)

        iac_grpd_ = {}

        for c in ['SIC', 'NAICS']:

            if c == 'NAICS':
                r_l, r_u = 310000, 399999

            if c == 'SIC':
                r_l, r_u = 2011, 3999

            iac_grpd[c] = self.iac[
                (self.iac.PRODHOURS.notnull()) &
                (self.iac[c].between(r_l, r_u))][
                    ['FY', c, 'EMPLOYEES', 'PRODLEVEL', 'PRODUNITS']
                    ].groupby(c)

        # Summarize



    def prodhours_ols(self, ind_code):
        """
        Run ordinary least squares (OLS) regression for the relationship
        between number of employees and production hours by industry codes
        (i.e., ind_code, SIC or NAICS). Saves results as csv file.
        """

        # Only include facilities that report annual production hours between
        # 1000 and 8760 and facilities that report the number of employees.
        ols_data = self.iac[
            (self.iac.PRODHOURS.between(1000, 8760)) &
            (self.iac.EMPLOYEES.notnull())
            ][['FY', ind_code, 'EMPLOYEES', 'PRODHOURS', 'PRODLEVEL',
               'PRODUNITS', 'ID']]

        # only include industry codes with 10 or more observations. This leaves
        # 348 SICs and , but not all are manufacturing industries.
        ols_ind_code = ols_data.groupby(ind_code, as_index=False).ID.count()

        ols_ind_code = ols_ind_code[ols_ind_code.ID >= 10]

        self.ols_data = pd.merge(ols_data, ols_ind_code, on=ind_code,
                                 how='right')

        # Loop through SICs, performing OLS and saving results
        results_list = []

        for code in self.ols_data[ind_code]unique():

            x = self.ols_data[self.ols_data[ind_code] == code].EMPLOYEES.values

            y = self.ols_data[self.ols_data[ind_code] == code].PRODHOURS.values

            ind_code_count = \
                self.ols_data[self.ols_data[ind_code] == code].ID_y.values[0]

            slope, intercept, r_value, p_value, std_error = \
                stats.linregress(x, y)

            results_list.append(
                [code, ind_code__count, slope, intercept, r_value, p_value,
                 std_error]
                )

        ols_results = pd.DataFrame(
            results_list, columns=[ind_code, 'code_count', 'slope', 'intercept',
                                   'r_value', 'p_value', 'std_error']
            )

        ols_results['r_squared'] = ols_results.r_value**2

        ols_results.to_csv('ols_results_emp_prodhours.csv')

    def test_normality(self, ind_code, alpha=0.05):
        """
        Calculate the Shapiro-Wilk (S-W) test that production hours by industry
        code (i.e., ind_cod, SIC or NAICS) are normally distributed. S-W tests
        the null hypothesis that the data was drawn from a normal distribution.
        Test reults below the chosen alpha mean that the null hypothesis of
        normality can be rejected.
        """

        results_list = []

        # Loop through industry codes, calculating S-W test.
        for code in self.ols_data[ind_code].unique():

            sw_stat, p_val = stats.shapiro(
                self.ols_data[self.ols_data[ind_code] == code].PRODHOURS.values
                )

            code_count = \
                self.ols_data[self.ols_data[ind_code] == code].ID_y.values[0]

            results_list.append([ind_code, code_count, sw_stat, p_val])

        s_w_results = pd.DataFrame(
            results_list, columns=[ind_code, code_count, 'S-W', 'p_value']
            )

        sig_count = len(s_w_results[s_w_results.p_value >=0.05])

        print(sig_count, "or", sig_count/len(s_w_results),
            "of industry codes may have normally-distributed \n",
            "production hours.")

        s_w_results.to_csv('sw_normality_prodhours.csv')

        # For p-values above the specified alpha, calculate normal distribution
        # parameters
        sic_dist_params = []

        for code in sw_results[sw_results.p_value >= alpha][ind_code]values:

            loc, scale = stats.norm.fit(
                self.ols_data[self.ols_data[ind_code] == code].PRODHOURS.values
                )

            indcode_dist_params.append([code, loc, scale])

        indcode_dist_results = pd.DataFrame(
            indcode_dist_params, columns=[ind_code, 'loc_param', 'scale_param']
            )



# What about creating bins by production hours that correspond to a set
# weekly schedule (e.g., ~2080 hours/year == 1 shift, 5 days/week)
# Are SICs detailed enough to use, or should they be mapped to 6-digit NAICS?
