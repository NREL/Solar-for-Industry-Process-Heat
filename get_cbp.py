import requests
import pandas as pd
import numpy as np
import io
import os
import zipfile
import json
from bs4 import BeautifulSoup
import re

class CBP:

    def __init__(self, year):

        f = open(
            'Censu_data_2014_cbp_variables_NAICS2012_values.html',
            'r'
            )

        naics2012_soup = BeautifulSoup(f, "lxml")

        naics2012_table_html = naics2012_soup.find_all('table')[0]

        naics2012_df = pd.DataFrame(columns=['naics', 'desc'],
            index=range(0, len(naics2012_table_html.find_all('tr'))))

        row_marker = 0

        for row in naics2012_table_html.find_all('tr'):
            column_marker = 0
            columns = row.find_all('td')
            for column in columns:
                naics2012_df.iat[row_marker, column_marker] = \
                    column.get_text()
                column_marker += 1
            row_marker += 1

        naics2012_df.dropna(inplace=True)

        naics2012_df.loc[:, 'desc'] = naics2012_df.desc.apply(
            lambda x: x.split("\n")[0]
            )

        naics2012_df.loc[:, 'n_naics'] = naics2012_df.naics.apply(
            lambda x: len(x)
            )

        naics2012_df = naics2012_df[(naics2012_df.n_naics == 6)]

        naics2012_df.loc[:, 'naics'] = naics2012_df.naics.apply(
            lambda x: int(x)
            )

        naics2012_df = \
            naics2012_df[naics2012_df.naics.between(1, 400000)]

        naics2012_df.reset_index(inplace=True, drop=True)

        naics2012_df.drop(['n_naics'], axis=1, inplace=True)

        self.naics2012 = naics2012_df

        year = str(year)

        user = os.getenv('username')

        cbp_file = 'cbp' + year[2:] + 'co'

        zip_path = 'C:\\Users\\' + user + '\\Desktop\\'

        cbp_csv_url = \
            'https://www2.census.gov/programs-surveys/cbp/' + \
            'datasets/' + year + '/' + cbp_file + '.zip'

        # first check if file exists
        if cbp_file + '.zip' not in os.listdir(zip_path):

            zip_cbp =  zipfile.ZipFile(
                io.BytesIO(
                    requests.get(cbp_csv_url).content
                    )
                )

            zip_cbp.extractall(zip_path)

        cbp = pd.read_csv(zip_path + cbp_file + '.txt')

        # NAICS codes are a strings that include - and / characters
        def fix_naics(naics):
            """
            Regex to retain only digits in naics strings. Returns
            integer.
            """
            if re.match("\d+", naics) is None:
                naics = 0

            else:
                naics = int(re.findall("\d+", naics)[0])

            return naics

        cbp.naics = cbp.naics.apply(fix_naics)

        # Keep only manufacturing NAICS codes
        cbp = cbp[(cbp.naics.between(300000, 400000)) |
                  (cbp.naics.between(30000, 40000)) |
                  (cbp.naics.between(3000, 4000)) |
                  (cbp.naics.between(300, 400))]

        # Create concatentated FIPS field to match GHGRP COUNTY_FIPS
        def state_fips_str(x):

            if len(str(x)) == 1:

                fips = '0' + str(x)

            else:
                fips = str(x)

            return fips

        def county_fips_str(x):

            int_len = len(str(x))

            mult = 3 - int_len

            fips = '0' * mult + str(x)

            return fips

        cbp.loc[:, 'COUNTY_FIPS'] = \
            cbp.fipstate.apply(state_fips_str) + \
                cbp.fipscty.apply(county_fips_str)

        census_regions = pd.read_csv(
            'C:\\Users\\cmcmilla\\' + \
            'Solar-for-Industry-Process-Heat\\' + \
            'US_FIPS_Codes.csv', index_col=['COUNTY_FIPS']
            )

        cbp.loc[:,'region'] = cbp.fipstate.map(
            dict(census_regions[
                ['FIPS State', 'MECS_Region']
                ].values)
            )

        cbp.reset_index(drop=True, inplace=True)

        # Create employment size categories that match MECS
        cbp.loc[:, 'Under 50'] = cbp[
            ['n1_4', 'n5_9', 'n10_19', 'n20_49']
            ].sum(axis=1)

        cbp.loc[:, '1000 and Over'] = cbp[
            ['n1000','n1000_1','n1000_2','n1000_3','n1000_4']
            ].sum(axis=1)

        self.cbp = cbp

# It'd be nice to use the Census API, but querying all counties for a given
# state results in an error about exceeding cell limit.
# This method loops through all states and their counties, but it takes
# too long to complete.
#API_auth_path = "U:/API_auth.json"
#
#with open(API_auth_path, 'r') as f:
#        auth_file = json.load(f)
#
#api_key = auth_file['census_API']

#naics2012_soup = BeautifulSoup(requests.get(
#    'https://api.census.gov/data/2014/cbp/variables/NAICS2012/values.html'
#    ).content, "lxml")
# def get_cbp(year, api_key):
#     year= str(2014)
#
#     fips = pd.read_excel(
#         'https://www2.census.gov/programs-surveys/popest/geographies/' +
#         year + '/all-geocodes-v' + year + '.xls', header=None
#         )
#
#     fips.dropna(inplace=True)
#
#     fips = pd.DataFrame(fips.values, columns=fips.iloc[0, :]).iloc[1:, :]
#
#     # Methods for converting FIPS values back to text
#     # Can't get dtype or converter options in pd.read_excel to preserve
#     # the original spreadsheet values in the resulting DataFrame.
#     def state_fips_str(x):
#         if len(str(x)) == 1:
#             fips = '0' + str(x)
#
#         else:
#             fips = str(x)
#
#         return fips
#
#     def county_fips_str(x):
#
#         int_len = len(str(x))
#
#         mult = 3 - int_len
#
#         fips = '0' * mult + str(x)
#
#         return fips
#
#     fips.loc[:, 'State Code (FIPS)'] = fips['State Code (FIPS)'].apply(
#         lambda x: state_fips_str(x)
#         )
#
#     fips.loc[:, 'County Code (FIPS)'] = fips['County Code (FIPS)'].apply(
#         lambda x: county_fips_str(x)
#         )
#
#     fips.loc[:, 'state_county'] = fips['State Code (FIPS)'].add(
#         fips['County Code (FIPS)']
#         )
#
#     fips.drop_duplicates(subset=['state_county'], inplace=True)
#
#     fips.to_csv('C:\\Users\\cmcmilla\\desktop\\test.csv')
#
#     fips_dict = {}
#
#     for i in fips['State Code (FIPS)'].unique():
#
#         if type(
#             fips.set_index('State Code (FIPS)')['County Code (FIPS)'].loc[i]
#             ) == np.str:
#
#             pass
#
#         else:
#
#             fips_dict[i] = [x for x in fips.set_index('State Code (FIPS)')[
#                 'County Code (FIPS)'
#                 ].loc[i].values]
#
#     cbp_list = []
#
#     for k, v in fips_dict.items():
#
#         for l in v:
#
#             if l == '000':
#
#                 continue
#
#             else:
#                 cbp_api = \
#                     'https://api.census.gov/data/' + year + '/cbp?get=' + \
#                     'GEO_ID,EMPSZES,NAICS2012,LFO_TTL,GEO_TTL' + \
#                     '&for=county:' + l + '&in=state:' + k + '&key=' + api_key
#
#                 cbp_list.append(requests.get(cbp_api).json())
#
#             print(cbp_api)
# def import_seed(file_path):
#     """
#     Imports and formats seed for manufacturing IPF from specified path.
#     """
#
#     def ft_split(s):
#         """
#         Handles splitting off fuel types with more than one word.
#         """
#         split = s.split('_')
#
#         ft = split[1]
#
#         for n in range(2, len(split)-1):
#
#             ft = ft + '_' + split[n]
#
#         return ft
#
#     seed_df = pd.read_csv(file_path, index_col=None)
#
#     seed_df = seed_df.replace({0:1})
#
#     seed_cols = [seed_df.columns[0]]
#
#     for c in seed_df.columns[1:]:
#         seed_cols.append(int(c))
#
#     seed_df.columns = seed_cols
#
#     seed_df.loc[:, 'region'] = seed_df.iloc[:, 0].apply(
#         lambda x: x.split('_')[0]
#         )
#
#     seed_df.loc[:, 'Fuel_type'] = seed_df.iloc[:, 0].apply(
#         lambda x: ft_split(x)
#         )
#
#     seed_df.loc[:, 'EMPSZES'] = seed_df.iloc[:, 0].apply(
#         lambda x: x.split('_')[-1]
#         )
#
#     return seed_df
#
# # %%
# def seed_correct_cbp(seed_df, cbp):
#     """
#     Changes seed values to zero based on CBP empolyment size count by
#     industry and region.
#     """
#
#     seed_df.set_index(['region', 'EMPSZES'], inplace=True)
#
#     # Reformat CBP data
#     cbp_pivot = cbp.copy(deep=True)
#
#     cbp_pivot.rename(columns={"n50_99": "50-99", "n100_249": "100-249",
#                               "n250_499": "250-499", "n500_999": "500-999"},
#                      inplace=True)
#
#     cbp_pivot = cbp_pivot.melt(id_vars=['region', 'naics'],
#                                value_vars=['Under 50', '50-99', '100-249',
#                                            '250-499', '500-999', 'Over 1000'],
#                                 var_name='EMPSZES')
#
#     cbp_pivot = pd.pivot_table(cbp_pivot, index=['region', 'EMPSZES'],
#                                columns=['naics'], values=['value'],
#                                aggfunc='sum')
#
#     cbp_pivot.columns = cbp_pivot.columns.droplevel()
#
#     shared_cols = []
#
#     for c in cbp_pivot.columns:
#         if c in seed_df.columns:
#             shared_cols.append(c)
#
#     cbp_mask = cbp_pivot[shared_cols].reindex(seed_df.index).fillna(0)
#
#     seed_df.update(seed_df[shared_cols].where(cbp_mask != 0, 0))
#
#     return seed_df
#
# seed = seed_correct_cbp(seed, cbp)
# # %%
# def seed_correct_MECS(seed_df, table3_2):
#     """
#     Changes seed values to zero based on MECS fuel use by industry and
#     region.
#     """
#
#     seed_df.reset_index(inplace=True, drop=False)
#
#     melt_cols = [0]
#
#     for n in range(3, len(table3_2.columns)):
#         melt_cols.append(n)
#
#     table3_2_mask = pd.pivot_table(
#         table3_2.iloc[:, melt_cols].melt(
#             id_vars=['region', 'naics'], var_name=['Fuel_type']
#             ),
#         index=['region', 'Fuel_type'], columns='naics'
#         )
#
#     table3_2_mask.columns = table3_2_mask.columns.droplevel()
#
#     seed_df.reset_index(drop=False, inplace=True)
#
#     seed_df.set_index(['region','Fuel_type'], inplace=True)
#
#     table3_2_mask = table3_2_mask.reindex(seed_df.index)
#
#     shared_cols = []
#
#     for c in table3_2_mask.columns:
#         if c in seed_df.columns:
#             shared_cols.append(c)
#
#     table3_2_mask = table3_2_mask[shared_cols].reindex(
#         seed_df.index
#         ).fillna(0)
#
#     seed_df.update(seed_df[shared_cols].where(table3_2_mask != 0, 0))
#
#     seed_df.reset_index(drop=True, inplace=True)
#
#     seed_df.drop(['EMPSZES'], axis=1, inplace=True)
#
#     return seed_df
#
# # %%
#
# seed_df = import_seed('IPF_seed.csv')
#
# seed_df = seed_correct_cbp(seed_df, cbp)
#
# seed_df = seed_correct_MECS(seed_df, table3_2)
