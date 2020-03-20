# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 13:16:33 2020

@author: wxi
"""
import re
import os
from abc import ABCMeta, abstractmethod
import get_API_params as gap
import datetime
import pandas as pd
# import models - financial models for diff technology

class LCOH(metaclass=ABCMeta):
    today = datetime.datetime.now()
    path = "./calculation_data"
    @abstractmethod
    def __init__(self):

        print("Initialize variables that require user input/format")

    @abstractmethod
    def __iter__(self):

        print("Used for process parity iteration")

    @abstractmethod
    def __next__(self):

        pass

    @abstractmethod
    def import_param(self):

        print("This method should contain every function used \
              to calculate all components of the LCOH equation \
              that aren't initialized.")

    @abstractmethod
    def calculate_LCOH(self):

        print("Use attributes to calculate LCOH")


class Greenfield(LCOH):

    # format -> add FIPS code, temperature, heat load

    def __init__(self, format):

        # process format
        f, self.temp_bin, self.load_bin = format

        self.invest_type, self.tech_type, self.iter_name = f.split(",")

        # define year for data imports
        self.year = LCOH.today.year

        # FIPS to Abbrev to State
        fips_data = pd.read_csv(os.path.join(LCOH.path, "US_FIPS_Codes.csv"),
                                usecols=['State', 'COUNTY_FIPS', 'Abbrev'])

        # get county
        while True:

            try:
                self.county = str(input("Enter a FIPS code or USA: ")).strip()

                if self.county not in fips_data['COUNTY_FIPS'].values:

                    raise AssertionError("No Such County")

                break

            except AssertionError as e:

                print(e)

        # get state_name, state_abbr
        self.state_name = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'State'].values[0].strip()

        self.state_abbr = fips_data.loc[fips_data['COUNTY_FIPS'] == self.county,
                                        'Abbrev'].values[0].strip()

        # Initialize appropriate attributes based on iteration variables
        self.capital = 0

        self.fuel_price, self.fuel_year = gap.UpdateParams.get_fuel_price(self.county)

        while True:

            try:
                self.p_time = int(input("Enter period of analysis (int): "))

                break

            except TypeError or ValueError:

                print("Please enter an integer.")

                continue

        self.discount_rate = 0.05

    def __iter__(self):

        return self

    def __next__(self):

        # pass in updated iter_variable to get updated LCOH
        pass

    def __str__(self):

        return "A LCOH object of investment {} for a {} and iterating {}".\
            format(self.invest_type, self.tech_type, self.iter_name)

    def import_param(self):

        print("this method will read the format to import appropriate params")

        # Execute script to get the iteration parameter

        def get_subsidies():

            """
            Returns of a dic of subsidies in the form of
            {year0: value, pinvest: lambda, time: lambda}
            year0 = fixed subsidies at year 0, %invest is a percentage of
            investment and time = time-dependent function

            """

            if self.tech_type in ['BOILER', 'FURNACE', 'KILN', 'OVEN']:

                return {"year0": 0, "pinvest": 0, "time": lambda t: t*0}

            else:
                """ For now to simplify, assume just the ITC tax credit """

                sol_subs = pd.read_csv(os.path.join(LCOH.path, "Sol_Subs.csv"),
                                       index_col=['State'])

                return {"year0": 0, "time": lambda t: t*0}

        self.subsidies = get_subsidies()

        def get_corptax():

            """
            Returns combined corporate tax rate - assume investment
            is always taxed at highest bracket (conservative)

            https://www.taxpolicycenter.org/statistics/state-corporate-income-tax-rates
            """
            corp_tax = pd.read_csv(os.path.join(LCOH.path, "corp_tax.csv"),
                                   index_col=['State'],
                                   usecols=['State', 'Total'])

            return corp_tax.loc[self.state_name, 'Total']

        self.corp_tax = get_corptax()

        def get_dep_value(t, no_years=5):

            """
            Please input number of years of schedule.
            Check https://www.irs.gov/pub/irs-pdf/p946.pdf

            """
            if (no_years == 3):
                schedule = [0.3333, 0.4445, 0.1481, 0.0741]
            elif (no_years == 5):
                schedule = [0.2, 0.32, 0.1920, 0.1152, 0.1152, 0.0576]
            elif (no_years == 7):
                schedule = [0.1429, 0.2449, 0.1749, 0.1249, 0.0893, 0.0892,
                            0.0893, 0.0446]
            elif (no_years == 10):
                schedule = [0.1, 0.18, 0.144, 0.1152, 0.0922, 0.0737, 0.0655,
                            0.0655, 0.0656, 0.0655, 0.0328]
            else:
                schedule = [0.2, 0.32, 0.1920, 0.1152, 0.1152, 0.0576]

            try:
                return schedule[t-1]

            except IndexError:

                return 0

        self.depreciation = get_dep_value

        def get_OM(t):

            """ Placeholders, OM fixed/var should be obtained from model.py"""

            # placeholder value
            OM_fixed_init = 10

            # placeholdervalue - not fuel
            OM_var_init_nf = 10

            OM_fuel = self.fuel_price*10

            # t just represents some function of time for now
            return OM_fixed_init*t + OM_var_init_nf*t + OM_fuel*t

        self.OM = get_dep_value

        def get_energy_yield(t):

            """placeholder going to use model.py file for this"""

            return 10

        self.energy = get_energy_yield

    def calculate_LCOH(self):

        """using general LCOH equation"""

        undiscounted = self.capital - self.subsidies["year0"]

        total_d_cost = 0

        t_energy_yield = 0

        for i in range(1, self.p_time+1):

            d_cost = (self.OM(i) * (1 - self.corp_tax) - self.capital *
                      self.depreciation(i) * self.corp_tax) / \
                      (1+self.discount_rate) ** i

            total_d_cost += d_cost

            energy_yield = self.energy(i) / (1 + self.discount_rate) ** i

            t_energy_yield += energy_yield

        self.LCOH_val = (undiscounted + total_d_cost)/t_energy_yield


class LCOHFactory():
    @staticmethod
    def create_LCOH(format):
        try:
            if re.search("GREENFIELD", format[0]):
                return Greenfield(format)
# =============================================================================
#             if re.search("EXTENSION",format):
#                 return Extension(format)
#             if re.search("REPLACE",format):
#                 return Replace(format)
# =============================================================================
            raise AssertionError("No Such LCOH Equation")
        except AssertionError as e:
            print(e)
