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

        # What about testing via OLS if there are trends in production hours
        # over time
