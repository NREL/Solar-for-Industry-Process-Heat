# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 14:38:43 2020

@author: wxi
"""

class ProcessParity:
    def __init__(self):
        pass
    def calc_LCOH(self,LCOH_type,a):
        """Determine LCOH for a given LCOH_type object"""
        LCOH_equation = LCOH_type()
        
    
class LCOH_type:
    b = 3
    def __init__(self,c):
        self.c = c
    def make_LCOH_equation(a):
        #a is the list of parameters to make the function
        def func1(self,c):
            #c would be the iter variable
            return a*self.b
        return func1
        
def test