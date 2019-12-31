
# Data
* Census quarterly survey (QPC) of avg. weekly operating hours
(~90 NAICS. Some are grouped together; 132 total).
* IAC database on annual operating hours by facility size (defined by
number of employees).
* EPA Air Markets Program data for hourly heat input to boilers and CHP/cogen
units (22 manufacturing NAICS).
* EPRI industrial electric load factors and national representative load shapes
by day type (ie, weekday, Saturday, and Sunday).


# Process
Overall intent is to develop a load shape generator (option for heat vs. elect?)
based on inputs of annual energy use, NAICS code, operating hours
(default to Census data), and employee size class. Additional flexibility for
specifying the load factor by industry and min and max of daily load shape
(represented relative to monthly peak load.)
## Steps
1. Test for seasonality in Census QPC data; use annual average where seasonality
does not exist.
* Use estimate standard errors to report high and low values for operating
hours.
2. Test for differences in IAC annual operating hours between employment size
classes by industry; results reported as operating hours relative to
industry mean.
3. Calculate monthly heat load factor by industry for boilers from EPA Air
Markets Program data.
* Calculate the coefficient of variation by hour by day type.
4. Allocate annual energy use to month using Census QPC data on avg weekly
operating hours.
5. Calculate monthly peak load using EPA data (mapped to QPC NAICS)

# Gaps
1. **NAICS and facility size diversity.** EPA Air Markets Program data cover only
22 NAICS codes. The facilities are likely all very large and operate
continuously.
2. **Lack of load shape resolution.** EPRI and EPA data represent total facility
load and do not disaggregate to the process level.
3. **Lack of process heat load shapes.** Boilers and CHP/cogen comprise most of
the combustion unit types in EPA data. EPRI data represent electrical loads
and electrical heating units are much less common for most industries than
combustion heating units. Therefore, the EPRI data may not adequately reflect
process heating operations.  
