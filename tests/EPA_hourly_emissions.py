
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
import urllib
from bs4 import BeautifulSoup

class EPA_AMD:

    def __init__(self):

        self.ftp_url = "ftp://newftp.epa.gov/DMDnLoad/emissions/hourly/monthly/"

        # Build list of state abbreviations
        ssoup = BeautifulSoup(
            requests.get("https://www.50states.com/abbreviations.htm").content,
            'lxml'
            )

        self.states = \
            [ssoup.find_all('td')[x].string.lower() for x in range(1,101,2)]

        self.months = ['01','02','03','04','05','06','07','08','09','10','11',
                       '12']



    def dl_and_format(self, years=range(1995, 2019)):
        """
        Download and format hourly load data for specified range of years.
        """

        # Get list of files in annual directory
        # iterate through files (zip), unzipping and reading
        def dl(

        #1995ri03.zip

        all_the_data = pd.DataFrame()

        for y in years:
            for state in states:
                for month in months:
            #source_address is a 2-tuple (host, port) for the socket to bind to as its source address before connecting
                    y_ftp_url = ftp_url+'{!s}/{!s}{!s}{!s}.zip'.format(
                        str(y),str(y),state,month
                        )

                    try:
                        response = urllib.request.urlopen(y_ftp_url)

                    except urllib.error.URLError as e:
                        print(e)
                        continue

                    # ftp_file = response.read()

                    zfile = ZipFile(BytesIO(response.read()))

                    hourly_data = pd.read_csv(zfile.open(zfile.namelist()[0]),
                                              low_memory=False)

                    if 'HEAT_INPUT' in hourly_data.columns:

                        hourly_data.dropna(subset=['HEAT_INPUT'],
                                           inplace=True)

                    if 'HEAT_INPUT (mmBtu)' in hourly_data.columns:

                        hourly_data.dropna(subset=['HEAT_INPUT (mmBtu)'],
                                           inplace=True)

                    usecols=['STATE','FACILITY_NAME','ORISPL_CODE','UNITID','OP_DATE',
                             'OP_HOUR','OP_TIME','GLOAD (MW)','SLOAD (1000lb/hr)', 'GLOAD', 'SLOAD'
                             'HEAT_INPUT (mmBtu)','FAC_ID','UNIT_ID','SLOAD (1000 lbs)']

                    drop_cols = set.difference(set(hourly_data.columns), usecols)

                    hourly_data.drop(drop_cols, axis=1, inplace=True)

                    all_the_data = all_the_data.append(hourly_data)
