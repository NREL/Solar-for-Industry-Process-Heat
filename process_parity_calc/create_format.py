# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 13:26:30 2020

@author: wxi
"""

class FormatMaker:
    """Creates formats to generate appropriate LCOH object"""
    
    def __init__(self):
        self.invest = ['REPLACE', 'GREENFIELD','EXTENSION']
        self.tech = ['FPC', 'CSP', 'PV+B', 'PV+R','PV+I',
                     'PV+HP','BOILER','CHP','FURNACE','KILN','OVEN']
        self.iter_var = ['INVESTMENT','FUELPRICE']
        self.param = ""
    
    def create_format(self):
    
        """Method to create the format for the factory object based off of investment and technology"""
        
        #get investment type
        
        all_invest = ', '.join(elem for elem in self.invest)
        all_tech = ', '.join(elem for elem in self.tech)
        all_iter = ', '.join(elem for elem in self.iter_var)
   
        
        while True:
            
            type_invest = input("Please input an investment type from " + all_invest + ": " )
            
            if str(type_invest).upper() in self.invest:
                
                break
        
        while True:
            
            type_tech = input("Please input a technology type from " + all_tech + ": ")
            
            if str(type_tech).upper() in self.tech:
                
                break 
        
        while True:
            
            type_iter = input("Please enter the process parity iteration parameter from " + all_iter + ": ")
            
            if str(type_iter).upper() in self.iter_var:
                
                break 
            
        while True:
            
            try:
                
                lower_T = float(input("Please enter the lower temp bound in Kelvins: "))
                
                upper_T = float(input("Please enter the upper temp bound in Kelvins: "))
                
                if lower_T<= 0 or upper_T<=0:
                    raise AssertionError ("Enter a Temperature (Kelvins) above 0")
                    
                if lower_T == upper_T:
                    
                    temp = lower_T
                    
                else:
                    
                    temp = [lower_T,upper_T]
                break
                    
            except ValueError: 
                
                print("That is not a number.")
                
            except AssertionError as e:
                
                print(e)
                
        while True:
            
            try:
                
                lower_load = float(input("Please enter the lower load req: "))
                upper_load = float(input("Please enter the upper load req: "))
                
                if lower_load<= 0 or upper_load<=0:
                    raise AssertionError ("Enter a load above 0")
                
                if lower_load == upper_load:
                    
                    load = lower_load
                    
                else:
                    
                    load = [lower_load,upper_load]               
                break
            
            except ValueError: 
                
                print("That is not a number.")
                
            except AssertionError as e:
                
                print(e)
                            
           
        self.param = (','.join([type_invest,type_tech,type_iter]), temp, load)
        
        return self.param