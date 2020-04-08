# Process Parity Analysis

This subdirectory contains code to calculate LCOH and determine process parity for different technology packages. 

## Prerequisites

eia-python v1.22 - https://pypi.org/project/eiapy/

dask v2.5.2 - https://docs.dask.org/en/latest/install.html

Rest of modules used should come with base installation. 

## Purpose 

The purpose of this subdirectory is to identify process parity when comparing solar vs combustion technologies for process heat. There is significant interest in identifying the solar investment costs associated with different fuel prices. The coding approach divides the calculation into 3 components: a model object, a LCOH object and a process parity object. 

The model object maps equipment specifications (heat load, etc.) to O&M/capital costs. 

The LCOH object contains all the relevant LCOH parameters and uses the imported model object attributes for LCOH calculations (just one LCOH or an entire MC simulation). 

The process parity object takes multiple LCOH objects (each with their own models) and operates on them to identify process parity. The process parity object will also produce graphs/visualizations for sensitivity and process parity analysis. 

## Code Overview

Process Parity (pr_par.py)

1. A process parity object is created. 

2. The process parity object creates two LCOH objects (solar + combustion tech) using a format string

3. Each LCOH object "imports" an appropriate model object using the format string

4. Run the process parity methods (pp_1D, pp_nD) to determine process parity for the two LCOH objects. 

5. Run the mc_analysis method to obtain MC results for each object, an analysis on the MC results and MC visualizations. 

LCOH Calculation (Create_LCOH_Obj.py)

1. An abstract class LCOH is used to ensure that future LCOH object addition by external users (when the code goes open source) has a base set of methods/attributes that must be defined for other parts of the code to work. 

2. Each LCOH object will have the following attributes after initialization:

	8760 load profile, average load, peak load, county (location), investment type (greenfield, replace, line extension)
	technology type(Boiler, PV+HP, CHP, etc.), a "date", FIPS to State map (csv file), heating values of different fuels, 
	fuel price for a given year, fuel escalation rate, period of analysis, discount rate, O&M escalation rate
	applicable subsidies, combined corporate tax rate(state + federal), depreciation structure
	annualized operating and capital costs
	
3. Each LCOH object will also contain a model object that provides the depreciation lifetime, technologies, cost indices, efficiencies,    O&M and capital costs. 

Model Object (models.py)

1. The model object is created using a format string generated in the LCOH object. 

2. It uses an abstract class to ensure that future model additions has a base set of methods/attributes that must be defined. 

3. The model structure is free - it depends on the models that you build. There can be many different types of models (reading from csv  	files, equations in the code, etc.). The only point of the object is to determine O&M and capital costs. 
   
## General Assumptions and Details by File

Things that need to be manually updated are:

Corporate tax dataset, Subsidies from DSIRE, MACRS depreciation schedule, Discount Rate, Cost Indices, Heat Content of Fuels, Fuel Escalation Rates

### bisection_method.py
It is sourced from UBC math. Standard bisection method without modifications. The code will be modified to search for an appropriate range to perform bisection method on. 

Source in docstring in py file. 

### create_cost_index.py
A script to create the cost index and export it as a csv file. Currently contains the producer price index for industrial chemicals and all the cost indexes from the chemical engineering cost index publication. Will be modified in the future to be able to input a year range for the cost indices. You currently need to provide the raw txt file manually and edit in the year to obtain your cost index file. 

Sources are in the docstrings in the py file. 
### create_format.py
Creates the format string used to initialize LCOH and model objects. It forces you to select from different options and has some simple user input error checking. 

### get_API_params.py
**Obtains lowest possible fuel price:**

Natural gas -> state-level industrial price vs National Henry Hub Spot Price
Coal -> Other industrial use price (state-level)
Petro (residual fuel) -> wholesale/resale price by all sellers (state-level) vs wholesale/resale price by refiners (national level)

The file grabs the national price and compares it to the state level price. It selects the national price if the state-level price is unavailable or if the state-level price is more than 2 years older. 

It assumes that the national price is the most up-to-date price available. It returns the fuel price and the year associated with the fuel price. 

**Obtains the fuel escalation:**
Obtains fuel escalation rate by state and fuel type using a csv file. The csv file is created manually using the EERC calculator. 

Calculator obtained at https://www.energy.gov/eere/femp/building-life-cycle-cost-programs

### pr_par.py
This file is still in progress. The current assumption is that during iteration only solar investment and combustion fuel costs can change. 

### create_LCOH_object.py

Assumptions:

	1. Average peak heat load for 8760. There is code to process the sample 8760 file but, it won't be implemented until 8760 finalized.
	
	2. The current year for analysis is the year that the code is being run in. 
	
	3. Heating values for coal, petroleum and natural gas are obtained manually from:
		https://www.eia.gov/totalenergy/data/monthly/#appendices spreadsheets
		
	4. Subsidies currently only use solar ITC
	
	5. Assume state-level corporate tax is always at the highest bracket and obtained manually from
		https://www.taxpolicycenter.org/statistics/state-corporate-income-tax-rates
		
	6. MACRS depreciation obtained from https://www.irs.gov/pub/irs-pdf/p946.pdf. If the class lifetime isn't in (3,5,7,10) use straight 		line depreciation.
	
	7. Fuel costs currently assume that the technology operates at the highest efficiency in the efficiency parameter range. 
	
	8. Uses general LCOH equation in strawman presentation term for term. 
	
	9. Distributions for MC parameters are obtained from sources. Parameter ranges are also obtained from sources + intuition. 

### models.py
Still a work in progress. Boiler model description in detail below

## Boiler Model Description
Some details of sources/models are in the docstring. This section will contain more details, assumptions and discussion on the boiler model. 

### Efficiency
Efficiencies for different fuel types obtained from: https://iea-etsap.org/E-TechDS/HIGHLIGHTS%20PDF/I01-ind_boilers-GS-AD-gct%201.pdf
Currently efficiencies are assumed to be the highest value (full load) - this will change once 8760 file processing implementation is done. Then the efficiency will be a linear interpolation between min and peak load. The heat load defined in this boiler is by input heat load the peak heat load is divided by efficiency to obtain input heat load. 

### Boiler Model Sources
All the cost curves for different boiler types are obtained in a 1978 report from the EPA: Capital and Operating Costs for Industrial Boilers and Cost Equations for Industrial Boilers - https://www.epa.gov/nscep. 

The boilers here don't cover < 5 MMBTU/hr. Thus, a cost curve obtained from a table in an OSTI report (capital costs only) is used: https://www.osti.gov/biblio/797810. 

Since the O&M costs for < 5 MMBTU/hr are not available, a % of capital of 18% for fixed O&M is assumed. This value is obtained from the times model spreadsheet provided by Colin. 

### Boiler Model Concerns

Doesn't distinguish between residual oil / natural gas - they use the same cost equations. 

The model is relatively old - sanity checks were done using other cost estimations. The times model investment costs / PJ were used to check different boiler costs (all appropriately deflated). Likewise, the OSTI model was used as well. The costs are similar to within the same order of magnitude depending on the heat load (times/osti model seems to be linear in heat load while the EPA model isn't). In the future quotes will be used to further check the validity of using an old model. 

In addition, the model distinguishes between fire/water-tube boilers, different types of coal boilers, packaged vs field-erected. Currently, I am using this EPA model and not distinguishing between these different configurations and only identifying boiler based off of heat load and fuel type. This could have implications on costs (fire tubes generally cheaper than water tube, etc.). The question of using multiple packaged boilers vs 1 field erected boiler is also an area of investigation. Multiple packaged boilers can be significantly (50%) cheaper than 1 field erected boiler of equivalent load. The research question would be: what is an appropriate boiler size distribution for a particular heat load in each industry?

The times model approach was to use multiple boilers of same load specification or 1 large boiler: https://iea-etsap.org/docs/TIMES_Dispatching_Documentation.pdf

### General boiler assumptions

It is okay to assume peak load and max efficiency if it is okay to assume that all plants have modular boiler systems that can operate near peak efficiency. Higher efficiency with higher load is:  https://www1.eere.energy.gov/femp/pdfs/OM_9.pdf


### Boiler Selection
As mentioned, the type of boiler (Fire vs water tube) and the boiler construction (packaged vs field erected) is not distinguished. The boiler is selected entirely on fuel type and input heat load. Input heat load is determined by dividing the peak heat load by the appropriate efficiency.

The logic behind the selection is defined in the select_boilers function in the constructor. It returns a list of boiler index locations and the number of boilers for each index location. This index location is also used to obtain the appropriate O&M/capital costs for the O&M/capital cost dictionaries. 

Assumption: Keep selecting the max sized boiler until you can select boiler that is within the range of the boiler model (0-700). 
Minor assumptions: extend the packaged boiler upper model limits for coal from 60-74 and 150 to 199 for NG/Petro. 

### Cost index multipliers
Currently costs are deflated to the last year available in the cost index csv file (this may not be the current year). 
There could be more granularity with wrt indices. 

#### For the annualized costs:

| Cost Type     | Index         |
| ------------- | ------------- |
| Utilites/Chemicals  | PPI Chemicals |
| Ash Disposal  | General CE Index |
| Direct Labor  | CEPI - Construction Labor |
| Engineering Supervision  | CEPI - Engineering Supervision  |
| Maintenance Labor  | CEPI - Construction Labor  |
| Replacement Parts | CEPI - Equipment |
| Overhead Cost | General CEPI Index  |

#### For the capital costs:

| Cost Type     | Index         |
| ------------- | ------------- |
| Equipment  | CEPI - Equipment  |
| Installation  | CEPI - Equipment |
| Indirect Costs  | General CE Index |

### Annualized O&M Costs:
All the costs do not include fuel or taxes. Labor cost was cut by a factor of 0.539 as per recommendations from the EPA 1978 documentation for constructing a boiler in an existing plant rather than a new one. 

Assumed 0.5 shifts of labor for the installation of a new boiler. Labor is a significant cost for boiler operation and this number needs to be determined more realistically. 

Assume operating at 100% capacity (boiler use is being operated at peak capacity year round for 8760 hours). 

Ash content assumption of 6% for coal based off of default values in EPA model -> need to update

### Capital Costs:
Capital costs do not include land and working capital. 
A contingency of 20% is assumed (1.2 x multiplier on capital cost). 
Heating value of coal is obtained manually from https://www.eia.gov/totalenergy/data/monthly/#appendices





