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
import numpy as np
#from sklearn.linear_model import LinearRegression
from statsmodels.formula.api import ols
from scipy.stats import ttest_ind

class IAC:
    """
    Class for downloading, formatting, and analyzing IAC data.
    Specify first year of data, otherwise will use data from 2002.
    """

    def __init__(self, first_year=2002):

        self.iac_url = 'https://iac.university/IAC_Database.zip'

        req = requests.get(self.iac_url)

        zipfile = ZipFile(BytesIO(req.content))

        self.iac_data = pd.read_excel(zipfile.open(zipfile.namelist()[0]),
                                 sheet_name='ASSESS')

        # Filter out annual production hours that are below 1000 and above
        # 8760
        self.iac_data = self.iac_data[
            self.iac_data.PRODHOURS.between(1000, 8760)
            ]

        self.iac_data = self.iac_data[
            self.iac_data.FY.between(first_year, 2020)
            ]

        # create employment size class bins
        self.iac_data['Emp_size'] = pd.cut(self.iac_data.EMPLOYEES,
            [1, 50, 100, 250, 500, 1000, 1000000] , right=True,
            labels=['n1_49', 'n50_99', 'n100_249', 'n250_499', 'n500_999',
                    'n1000'],
            retbins=False, precision=3, include_lowest=True,
                    duplicates='raise')

        # Drop na values
        self.iac_data.dropna(subset=['Emp_size'], inplace=True)

        #Fill in missing NAICS codes by matching SIC codes.
        self.sic_naics = pd.read_csv(
            os.path.join('../calculation_data/', '1987_SIC_to_2002_NAICS.csv')
            )

        self.sic_naics.rename(columns={'2002 NAICS': 'NAICS'}, inplace=True)

        mapped_naics = pd.merge(self.iac_data[['SIC', 'ID']],
                                self.sic_naics[['SIC', 'NAICS']],
                                on=['SIC'], how='left')

        # Some SIC codes correspond to more than one NAICS.
        # First fill in missing NAICS for SICs with only one NAICS
        sic_single = self.sic_naics.groupby(
            'SIC', as_index=False
            )['NAICS'].count()

        sic_single = sic_single[sic_single['NAICS'] == 1]

        sic_single.rename(columns={'NAICS': 'NAICS_count'}, inplace=True)

        mapped_naics_single = pd.merge(mapped_naics, sic_single, on='SIC')

        self.iac_data.set_index('SIC', inplace=True)

        self.iac_data.NAICS.update(
            mapped_naics_single.set_index('SIC').NAICS.drop_duplicates()
            )

        self.iac_data.reset_index(inplace=True)

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

        sicgrpd = self.sic_naics.groupby('SIC')

        for sic in sicgrpd.groups:

            agg_naics.append([sic, check_naics_agg(sicgrpd.get_group(sic))])

        agg_naics = pd.DataFrame(agg_naics, columns=['SIC', 'NAICS'])

        #Update missing NAICS based on aggregated values
        agg_update = pd.DataFrame(self.iac_data[self.iac_data.NAICS.isnull()])

        agg_update['iac_index'] = agg_update.index

        agg_update.set_index('SIC', inplace=True)

        agg_naics.set_index('SIC', inplace=True)

        agg_update.update(agg_naics)

        agg_update.reset_index(inplace=True)

        agg_update.set_index('iac_index', inplace=True)

        agg_update.index.name = 'index'

        self.iac_data.update(agg_update.NAICS)

        iac_grpd_ = {}

        # for c in ['SIC', 'NAICS']:
        #
        #     if c == 'NAICS':
        #         r_l, r_u = 310000, 399999
        #
        #     if c == 'SIC':
        #         r_l, r_u = 2011, 3999
        #
        #     iac_grpd[c] = self.iac_data[
        #         (self.iac_data.PRODHOURS.notnull()) &
        #         (self.iac_data[c].between(r_l, r_u))][
        #             ['FY', c, 'EMPLOYEES', 'PRODLEVEL', 'PRODUNITS']
        #             ].groupby(c)

        # Remove entries without production hours and non-manufacturing
        # facilities.
        self.iac_data = self.iac_data[
                (self.iac_data.PRODHOURS.notnull()) &
                (self.iac_data.NAICS.between(310000, 399999))
                ]

        # Match NAICS to QPC NAICS codes
        qpc_naics = pd.read_csv(
            '../calculation_data/qpc_weekly_hours.csv', usecols=['NAICS']
            )

        qpc_naics = qpc_naics[qpc_naics !='31-33'].dropna()

        qpc_naics = qpc_naics.astype(int)

        iac_naics = pd.DataFrame(self.iac_data.NAICS.unique())

        iac_naics.columns = ['n6']

        iac_naics = pd.concat(
            [iac_naics.n6.apply(
                lambda x: int(str(x)[0:n])
                ) for n in range(6,3,-1)], axis=1
                )

        iac_naics.columns = ['n6', 'n5', 'n4']

        iac_match = iac_naics.apply(
            lambda x: x.isin(qpc_naics.NAICS),axis=0
            ).multiply(iac_naics)

        iac_match['iac_naics'] = iac_naics.n6

        iac_match.set_index('iac_naics', inplace=True)

        iac_match = pd.DataFrame(pd.concat(
            [iac_match.where(
                iac_match >0, np.nan\
                ).loc[:, c].dropna() for c in iac_match.columns],
            ignore_index=False, axis=0
            ))

        iac_match.columns = ['qpc_naics']

        self.iac_data = pd.merge(
            self.iac_data, iac_match, left_on='NAICS', right_index=True
            )

        # Create 3-digit NAICS field for aggregation
        self.iac_data['naics3'] = self.iac_data.NAICS.apply(
            lambda x: int(str(x)[0:3])
            )

test = IAC()

test.iac_data.head()

    def ttest_prodhours(iac_data, critval=0.05):
        """
        Run the T-test for the mean of one employment size class.

        Per scipy documentation, this is a two-sided test for the null
        hypothesis that 2 independent samples have identical average (expected)
        values. This implementation assumes that the samples do not have the
        same variance and sample size (Welch's t-test).

        Choice of Welch's may be problemmatic given overlapping nature of
        samples (e.g., all samples in a given employment class are also in
        the samples used to calculate total mean). An alternative is to
        run a one-sample t-test.
        """

        df = iac_data.set_index(['naics3', 'Emp_size'])

        emp_means = pd.DataFrame(
            df.PRODHOURS.mean(level=[0,1])
            ).reset_index().pivot(
                'naics3', 'Emp_size', 'PRODHOURS'
                )

        # emp_means = pd.DataFrame(
        #     np.nan, index=df.index.get_level_values(0).unique().sort_values(),
        #     columns=df.index.get_level_values(1).unique()
        #     )

        emp_means = pd.concat([df.PRODHOURS.mean(level=0), emp_means],
                              axis=1)

        emp_means.rename(columns={'PRODHOURS': 'all_mean'}, inplace=True)

        emp_means = pd.concat([df.PRODHOURS.count(level=0), emp_means],
                              axis=1)

        emp_means.rename(columns={'PRODHOURS': 'obs_count'}, inplace=True)

        for n in emp_means.index:

            ttest_res_ind = [ttest_ind(
                df.xs([n, emp]).PRODHOURS,
                df.xs([n]).PRODHOURS, equal_var=False
                )[1] for emp in emp_means.loc[n].dropna().index[2:]]

            # This is thetwo-sided test for the null hypothesis that the
            #expected value (mean) of a sample of independent observations is
            # equal to the given population mean.
            # ttest_res = [ttest_1samp(
            #     df.xs([n, emp]).PRODHOURS,
            #     emp_means.loc[n, 'all_mean']
            #     )[1] for emp in emp_means.loc[n].dropna().index[2:]]

            # Keep results only if result is below critical value
            ttest_res_ind = pd.DataFrame(
                np.multiply(emp_means.xs(n).dropna()[2:].values,
                            (pd.Series(ttest_res_ind) < 0.05)).values,
                index=emp_means.loc[n].dropna().index[2:], columns=[n]).T

            emp_means.update(ttest_res_ind)

        # Drop NAICS with obs < 50
        emp_means = emp_means.where(emp_means.obs_count>50)

        emp_means.dropna(inplace=True, thresh=1)

        # Report means by employment size class relative to group mean
        emp_means.iloc[:, 1:].update(
            emp_means.iloc[:, 1:].divide(emp_means['all_mean'], axis=0)
            )

        # Replace instances where there was no statistical significance with
        # 1, indicating the NAICS mean.
        # NaN values remain, indicating that there were no observations
        # for that size class and industry
        emp_means.replace(0, 1, inplace=True)

        # Use these results to multiply the QPC avg weekly op hours
        return emp_means


    def run_prodhours_ols(group):
        """
        Run ordinary least squares (OLS) regression for the relationship
        between number of employees and production hours by industry codes
        (i.e., ind_code, SIC or NAICS). Saves results as csv file.
        """

        ols_data = group

        # ols_data = pd.DataFrame(ols_data.sum(axis=1)).join(ols_data)
        #
        # ols_data.iloc[:, 1:] = ols_data.iloc[:, 1:].mask(~ols_data.isnull(), 1)
        #
        # ols_data.fillna(0, inplace=True)
        #
        # ols_fit = LinearRegression().fit(ols_data.iloc[:, 1:], ols_data[0])

        ols_final = pd.DataFrame(
            np.nan, columns=['n1_49', 'n50_99', 'n100_249', 'n250_499',
                             'n500_999', 'n1000'],
            index=[ols_data.qpc_naics.unique()[0]]
            )

        # Skip NAICS without observations from each emploument class?
        # if len(ols_data.)

        ols_fit = ols('PRODHOURS ~ C(Emp_size)', data=ols_data).fit()

        # Get p values of regression coefficients; can get those below 0.05
        if any(ols_fit.pvalues[1:] < 0.05):

            ols_res = pd.DataFrame(
                np.multiply(ols_fit.pvalues<0.05, ols_fit.params)
                )

            ols_res = ols_res[ols_res>0].T

            # ols_res.update(np.add(ols_res[1:], ols_res[0]))

            ols_res.rename(
                columns={'Intercept': 'n1_49', 'C(Emp_size)[T.n50_99]': 'n50_99',
                       'C(Emp_size)[T.n100_249]': 'n100_249',
                       'C(Emp_size)[T.n250_499]': 'n250_499',
                       'C(Emp_size)[T.n500-999]': 'n500_999',
                       'C(Emp_size)[T.n1000]': 'n1000'},
                index={0:ols_data.qpc_naics.unique()[0]}, inplace=True
                )

            ols_final.update(ols_res)

        # Not all NAICS have records with all employment size classes.
        # Mask the instances where there aren't entries.
        emp_mask = np.ma.make_mask(pd.DataFrame(
            ols_data.groupby('Emp_size').PRODHOURS.mean()
            ).T)

        ols_final = ols_final.multiply(emp_mask).fillna(0)

        return ols_final

iac_empsize = pd.concat(
    [run_prodhours_ols(
        test.iac_data[test.iac_data.qpc_naics == naics]
        ) for naics in test.iac_data.qpc_naics.dropna().unique()], axis=0,
    ignore_index=False
    )

# https://stackoverflow.com/questions/50733014/linear-regression-with-dummy-categorical-variables

        LinearRegression(fit_intercept=True, normalize=False, copy_X=True, n_jobs=None)[source]
        # Only include facilities that report annual production hours between
        # 1000 and 8760 and facilities that report the number of employees.
        ols_data = self.iac_data[
            (self.iac_data.PRODHOURS.between(1000, 8760)) &
            (self.iac_data.EMPLOYEES.notnull())
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
