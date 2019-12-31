
import requests
import numpy as np
import re
import pandas as pd
from bs4 import BeautifulSoup

fdir = 'c:/users/cmcmilla/Solar-for-industry-process-heat/CHP/'

matched_file = 'CHP with Counties_2012_NAICS.csv'

matched = pd.read_csv(fdir+matched_file, index_col=0)

# Some counties also have 0 values
matched = matched[
    (matched['FIPS County'].isnull()) | (matched['FIPS County']==0)
    ]
#
#70: 'Fresno', 45: 'Chandler', 107: 'Fresno'
# Fix aming inconsistencies
matched['Organization Name'] = matched['Organization Name'].apply(
    lambda x: x.replace(', Inc.', ' Inc.')
    )

CHPdb = pd.read_excel(
    'Y:/6A20/public/IEDB/Data/Energy Use/CHPDB_database_updated.xls',
    usecols=['Organization Name', 'City', 'Capacity (kW)', 'State', 'NAICS']
    )

CHPdb = CHPdb[~CHPdb.NAICS.isnull()]

#Keep only manufacturing industries (NAICS starting with 3)
CHPdb['N1'] = CHPdb.NAICS.apply(lambda x: int(str(x)[0]))

CHPdb = CHPdb[(CHPdb.N1 == 3)] #& (CHPdb.State != 'PR')]

# Drop where city isn't provided
CHPdb.dropna(subset=['City'], inplace=True)

CHPdb = CHPdb[CHPdb['Capacity (kW)'].notnull()]

CHPdb.drop(['N1', 'NAICS'], axis=1, inplace=True)

CHPdb.columns = ['Abbrev','City','Organization Name','Capacity (kW)']

CHPdb['Capacity (kW)'] = CHPdb['Capacity (kW)'].astype(int)

CHPdb['Organization Name'] = CHPdb['Organization Name'].apply(
    lambda x: x.replace(', Inc.', ' Inc.')
    )

matched = pd.merge(
    matched, CHPdb, on=['Organization Name', 'Capacity (kW)'], how='left'
    )

# Also drop Puerto Rican and Virgin Island facilities
matched = matched[matched.State <=56]

matched['FIPS County'] = np.nan

fips = pd.read_csv(
    'c:/users/cmcmilla/solar-for-industry-process-heat/calculation_data/' +\
    'US_FIPS_Codes.csv', usecols=['County_Name','COUNTY_FIPS','Abbrev'],
    index_col=['COUNTY_FIPS']
    )

fips['Abbrev'] = fips.Abbrev.apply(lambda x: x.strip())

fips['county_state'] = fips.apply(lambda x: tuple(x), axis=1)

fips.reset_index(inplace=True)

fips.set_index('county_state', inplace=True)

def find_county(city, state, fips):

    print(city, state)

    url = 'https://publicrecords.onlinesearches.com/zip-code-county/' +\
        'show?utf8=%E2%9C%93'

    pl = {'city_or_town': city, 'state_abbr': state, 'city_commit': 'Search'}

    try:

        r = requests.get(url, pl)

    except Exception as e:

        print(e)

        return np.nan

    soup = BeautifulSoup(r.content)

    try:

        txt = re.findall('([A-Z]\w+)', str(soup.tbody.a['href']))[0]

    except Exception as e:

        print(e)

        return np.nan

    txt = txt.split('_')[1]

    fixes = {'ContraCosta': 'Contra Costa', 'SanJoaquin': 'San Joaquin',
             'NewLondon': 'New London', 'SaltLake': 'Salt Lake',
             'Dupage': 'Du Page'}

    if txt in fixes.keys():

        txt = fixes[txt]

    county_state = (txt, state)

    return fips.xs(county_state).COUNTY_FIPS

for i in matched[matched.Abbrev.notnull()].index:
    matched.loc[i, 'FIPS County'] = \
        find_county(matched.loc[i, 'City'], matched.loc[i, 'Abbrev'], fips)

manual = {'Elsinore': 6065, 'Greenboro':13133, 'St. Gabriel': 22047,
          'St. Martinville': 22099, 'Iberia': 22045, 'St. Louis': 29510,
          'Sayerville': 34023, 'Wauna': 41007, 'Rowley': 49045,
          'Locks Mill': 55087, 'Natrium': 54051, 'Beaver Creek': 56039}

matched['FIPS County'].update(matched.City.map(manual))

matched_og = pd.read_csv(fdir+matched_file, index_col=0)

matched_og = matched_og.dropna(subset=['FIPS County'])

matched_og = matched_og[matched_og['FIPS County']!=0]

matched.drop(['Abbrev', 'City'], axis=1, inplace=True)

matched_og = matched_og.append(matched, ignore_index=True)

matched_og.to_csv(fdir+'CHP with Counties_2012_NAICS_updated2.csv')

summary = matched_og.groupby(
    ['FIPS County', 'NAICS_2012', 'Fuel Class - Primary Fuel'], as_index=False
    )['Capacity (kW)'].sum()

summary.to_csv(fdir+'CHP_county_summary_updated.csv')
