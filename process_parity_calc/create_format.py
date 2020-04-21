# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 13:26:30 2020

@author: wxi
"""
import pandas as pd
import os

class FormatMaker:
    """Creates formats to generate appropriate LCOH object"""

    def __init__(self):
        self.invest = ['REPLACE', 'GREENFIELD', 'EXTENSION']
        self.tech = ['FPC', 'CSP', 'PVB', 'PVR', 'PVI',
                     'PVHP', 'BOILER', 'CHP', 'FURNACE', 'KILN', 'OVEN']
        #self.iter_var = ['INVESTMENT', 'FUELPRICE']
        self.param = ""
        self.fips_data = pd.read_csv(os.path.join("./calculation_data", "US_FIPS_Codes.csv"),
                                usecols=['State', 'COUNTY_FIPS', 'Abbrev'])

    def create_format(self):

        """Create format for factory object."""

        # get investment type
        all_invest = ', '.join(elem for elem in self.invest)
        all_tech = ', '.join(elem for elem in self.tech)
        #all_iter = ', '.join(elem for elem in self.iter_var)

        while True:

            type_invest = input("Please input an investment type from " +
                                all_invest + ": ")

            if str(type_invest).upper() in self.invest:

                break

        while True:

            type_tech = input("Please input a technology type from " +
                              all_tech + ": ")

            if str(type_tech).upper() in self.tech:

                break
                # get county
                
        while True:

            county = str(input("Enter a FIPS code or USA: ")).strip()

            if county in self.fips_data['COUNTY_FIPS'].values:
                    
                break

            print("No such county.")

        while True:

            try:

                avg_load = float(input("Enter the average load (kW): "))

                if avg_load <= 0:
                    raise AssertionError("Enter a load above 0")
                
                break

            except ValueError:

                print("That is not a number.")

            except AssertionError as e:

                print(e)

        self.param = (','.join([type_invest, type_tech]).upper(),
                     avg_load, county)

        return self.param

if __name__ == "__main__":
    print(FormatMaker().create_format())
