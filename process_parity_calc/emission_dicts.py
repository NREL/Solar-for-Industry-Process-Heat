# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 19:04:37 2020

@author: wxi
criteria air pollutants only
stagger costs defined by ton of pollutants matrix -> lower, upper, fixed or variable, underscore_cap for limits
cap for a hard cap in max $ fee due to variable pollutants
spec_cap - max eligible tons per each pollutant (4 in total)
special -> special cases with fee structures that don't fit such as function of pollutant tons
Title V state-level permit program costs 
"""

state_costs = \
{
 "Alabama": {"variable": [68.5], "fixed": [0]},
 "Arizona": {"variable": [45.5], "fixed": [20000]},
 "Arkansas": {"variable": [23.93], "fixed": [0]},
 "California": {"variable": [124.51], "fixed": [0]},
 "Colorado": {"variable": [32], "fixed": [0]},
 "Connecticut": {"variable": [52.93], "fixed": [0], "min": 1000, "max": 500000},
 "Delaware": {"variable": [0],"fixed": [0], "stagger": 
     {"lower": [0,6,26,51,101,201,501,1001,2000],
      "upper": [5,25,50,100,200,500,1000,2000,9999999],
      "fixed": [3950,4100,6000,9000,12000,28000,60000,100000,350000]}},
 "Florida": {"variable": [30], "fixed": [0]},
 "Georgia": {"variable": [35.5], "fixed":[1900]},
 "Idaho": {"variable": [39.48], "fixed": [0], "stagger":
     {"lower": [0,200,500,1000,3000,4500,7000],
      "upper": [199,499,999,2999,4499,6999,9999999],
      "fixed": [3575,7150,11050,22750,28600,42900,71500],
      "variable_cap": [3575,10725,25025,35100,71500,143000,143000]}},
 "Illinois": {"variable": [0], "fixed":[0], "stagger":
     {"lower":[0,100,13674], "upper":[99,13673,9999999],
      "fixed" : [2150,0,294000], "variable": [0,21.5,0]}},
 "Indiana": {"variable": [41.25], "fixed": [0] , "cap": 187500},
 "Iowa": {"variable": [70], "fixed":[0], "spec_cap": 4000},
 'Kansas': {"variable": [37], "fixed":[0], "spec_cap": 4000},
 'Kentucky': {"variable": [52], "fixed": [0]},
 'Louisiana': {"variable": [32.21], "fixed": [0]},
 'Maine': {"variable":[0], "fixed":[0], "special": 
     lambda x: x*9.36 if (x<=1000) else (9360+(x-1000)*18.79 if ((x>1000) and (x<=4000)) else 9360 + 2999*18.79 + 28.11*(x-4000))},
 'Maryland': {"variable": [62.01], "fixed": [5000]},
 'Massachusetts': {"variable": [0], "fixed":[0], "stagger":
     {"lower": [0,100,250,5000],
      "upper": [99,249,4999,9999999],
      "fixed": [3000,5500,7500,100000],
      "variable": [6,8,12,25]}},
 'Michigan': {"variable": [0], "fixed":[0], "spec_cap": 1250, "stagger":
     {"lower": [0,6,60,200,2000],
      "upper": [5,59,199,1999,9999999],
      "fixed": [5250,7500,10500,15750,21000],
      "variable": [53,53,53,53,53]}},
 'Minnesota': {"variable": [115.32], "fixed": [0]},
 'Mississippi': {"variable": [47], "fixed": [0], "spec_cap": 4000},
 'Missouri': {"variable": [48], "fixed": [0], "spec_cap": 4000, "cap": 48*12000},
 'Montana': {"variable": [44.35], "fixed": [900]},
 'Nebraska': {"variable": [78], "fixed":[0], "spec_cap": 4000},
 'Nevada': {"variable": [16.98], "fixed": [30000]},
 'New Hampshire': {"variable": [217.5], "fixed": [500]},
 'New Jersey': {"variable": [122.45], "fixed": [0]},
 'New Mexico': {"variable": [32.15], "fixed": [0]},
 'New York': {"variable": [0], "fixed": [2500], "spec_cap": 7000, "stagger":
     {"lower": [0,1000,2000,5000],
      "upper": [999,1999,4999,9999999],
      "variable": [60,70,80,90]}},
 'North Carolina': {"variable": [34.25], "fixed": [7423]},
 'North Dakota': {"variable": [15.91], "fixed": [0]},
 'Ohio': {"variable": [52.03], "fixed": [0]},
 'Oklahoma': {"variable": [39.86], "fixed": [0]},
 'Oregon': {"variable": [15.22], "fixed": [2013]},
 'Pennsylvania': {"variable": [85], "fixed": [0], "spec_cap": 4000},
 'Rhode Island': {"variable": [52.03], "fixed": [0]},
 'South Carolina': {"variable": [52.03], "fixed":[0], "stagger":
     {"lower": [0,10,51,101,251,1001],
      "upper": [9,50,100,250,1000,9999999],
      "fixed": [500,1000,2000,3500,6500,10000]}},
 'South Dakota': {"variable": [40], "fixed": [1000]},
 'Tennessee': {"variable": [53.5], "fixed": [4000]},
 'Texas': {"variable": [53.34], "fixed": [0]},
 'Utah': {"variable": [82.75], "fixed": [0]},
 'Vermont': {"variable": [67], "fixed": [1500]},
 'Virginia': {"variable": [85.43], "fixed": [0], "spec_cap": 4000},
 'Washington': {"variable": [43.31], "fixed": [25997]},
 'West Virginia': {"variable": [53.32], "fixed": [0], "spec_cap": 4000},
 'Wisconsin': {"variable": [35.71], "fixed": [3000]},
 'Wyoming': {"variable": [34.5], "fixed": [0]}
}
# federal costs is per ton only for those registered under cfr part 71
federal_costs = 53.81

# add in emission per heat input rate limits based off word doc
# (lower input fuel, upper input fuel, fuel type, emission type, emission limit (ng/J), amount of fuel)
#LOW = <0.3 coal, mixed is above <0.9 coal, pure is pure fuel ANY is any amount -> only specified for coal
# just assume wood to be 43 now -> can add more detail if we're really analyzing pulp industry
#(250,9999999, "NG or OIL", "SO2", 26), #if this condition is met PM EXEMPT ELSE

# =============================================================================
# emission_limits = \
# [(0,9999999, "COAL", "PM", 22, "PURE"), 
#  (0,9999999, "COAL", "PM", 43, "MIXED"), 
#  (0,99, "COAL", "PM", 43, "LOW"), 
#  (100,9999999, "COAL", "PM", 86, "LOW"), 
#  (0,9999999, "WOOD", "PM", 43), 
#  (250,9999999, "NG", "PM", 13),
#  (250,9999999, "OIL", "PM", 13),
#  (100,9999999, "NG", "NOX", 86),
#  (100,9999999, "COAL", "NOX", 300),
#  (100,9999999, "OIL", "NOX", 129),
#  (0,250, "COAL", "SO2", 87, "ANY"),
#  (251,9999999, "COAL", "SO2", 65, "ANY"),
#  (0,99, "OIL", "SO2", 215)
#  (100, 250, "OIL", "SO2", 87),
#  (251,9999999, "OIL", "SO2", 86),
#  (250,9999999, "NG", "SO2", 86)]
# =============================================================================

# =============================================================================
# emission_fac = \
# {"COAL": {[(0,99,{})}}
# =============================================================================

 