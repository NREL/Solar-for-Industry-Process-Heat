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
        self.tech = ['DSGLF', 'PTC', 'PTCTES', 'SWH', 'PVEB', 'PVRH', 'PVWHR',
                     'PVHP', 'BOILER', 'CHP', 'FURNACE', "EBOILER"]
        #self.iter_var = ['INVESTMENT', 'FUELPRICE']
        self.param = ""
        self.fips_data = pd.read_csv(os.path.join("./calculation_data", "US_FIPS_Codes.csv"),
                                usecols=['State', 'COUNTY_FIPS', 'Abbrev'])

    def create_format(self, iter_dict = False):
        
        invest = True
        tech = True
        county = True
        mult = True

        """Create format for factory object."""
        if iter_dict:
            for k in iter_dict.keys():
                if k.upper() == "SOLARM":
                    multiplier = iter_dict[k]
                    if not all(i >=0 for i in multiplier):
                        raise AssertionError("Negative solar multiplier")
                    load = False
                elif k.upper() == "COUNTY":
                    county_no = list(map(str,iter_dict[k]))
                    if not (set(county_no) <= set(self.fips_data['COUNTY_FIPS'].values)):
                        raise AssertionError("Invalid County Number")
                    county = False
                elif k.upper() == "TECH":
                    type_tech = list(map(lambda x: x.upper(), iter_dict[k]))
                    if not (set(type_tech) <= set(self.tech)):
                        raise AssertionError("Not a valid technology type.")
                    tech = False
                elif k.upper() == "INVEST":
                    type_invest = list(map(lambda x: x.upper(), iter_dict[k]))
                    if not (set(type_invest) <= set(self.invest)):
                        raise AssertionError("Not a valid investment type.")
                    invest= False
                else:
                    print("Not a valid dictionary.")
                

            
        # get investment type
        all_invest = ', '.join(elem for elem in self.invest)
        all_tech = ', '.join(elem for elem in self.tech)
        #all_iter = ', '.join(elem for elem in self.iter_var)

        while invest:

            type_invest = input("Please input an investment type from " +
                                all_invest + ": ")

            if str(type_invest).upper() in self.invest:

                break

        while tech:

            type_tech = input("Please input a technology type from " +
                              all_tech + ": ")

            if str(type_tech).upper() in self.tech:

                break
                
        while county:

            county_no = str(input("Enter a FIPS code or USA: ")).strip()

            if county_no in self.fips_data['COUNTY_FIPS'].values:
                    
                break

            print("No such county.")

        while mult:

            try:
                message = ' For solar technologies enter -1 for auto-sizing, 0 for no solar, positive number for 1 MW multiplier. For combustion technologies enter -1:  '
                multiplier = float(input(message))

                if multiplier < -2:
                    raise AssertionError("Enter an appropriate multiplier: ")
                
                break

            except ValueError:

                print("That is not a number.")

            except AssertionError as e:

                print(e)
        
        variables = [type_invest, type_tech, multiplier, county_no]
        
        if any([type(var) == list for var in variables]):
            length = []
            notlist_ind = []
            for i,var in enumerate(variables):
                if type(var) == list:
                    length.append(len(var))
                else:
                    notlist_ind.append(i)
            try:
                if len(set(length)) != 1:
                    raise AssertionError("List of iterables are not the same length")
                    
            except AssertionError as e:
                
                print(e) 
                
            for i in notlist_ind:
                
                variables[i] = [variables[i]] * length[0]
        
            self.param = [[a.upper(),b.upper(),c,d] for a,b,c,d in zip(variables[0], variables[1], variables[2], variables[3])]
          
        else:

            self.param = [(type_invest.upper(), type_tech.upper(), multiplier, county_no)]

        return self.param

if __name__ == "__main__":
    print(FormatMaker().create_format({"county": [2290,
 1133,
 5149,
 4027,
 6115,
 8125,
 9015,
 11031,
 10005,
 12133,
 13321,
 15009,
 19197,
 16087,
 17203,
 18183,
 20209,
 21239,
 22127,
 25027,
 24510,
 23031,
 26165,
 27173,
 29510,
 28163,
 30111,
 37199,
 38105,
 31185,
 33019,
 34041,
 35061,
 32510,
 36123,
 39175,
 40153,
 41071,
 42133,
 44009,
 45091,
 46137,
 47189,
 48507,
 49057,
 51840,
 50027,
 53077,
 55141,
 54109,
 56045]}))
