# Summary
Technical Opportunity
: The fraction of process heat demand that can be provided by solar technologies, given available land and solar resources.

## Description of Files and Directories
* `run_tech_opp_parallel.py`: Used to run the technical opportunity calculations for a technology package. Default setup is to run calculations for all relevant counties using the `multiprocessing` package. Saves results as `hdf5` file.
* `tech_opp_calcs_parallel.py`: Performs the technical opportunity calculation by dividing the hourly scaled solar generation by the hourly IPH demand.
* `tech_opp_demand.py`: Formats the process demand results and creates 8760 load using the `demand_hourly_load` method from `/heat_load_calculations/run_demand_8760.py`
* `format_rev_output.py`: Formats `.h5/` file from [reV](https://github.com/NREL/reV) runs of [pySAM](https://nrel-pysam.readthedocs.io/en/v1.2.dev3/index.html) for 1-MW equivalent photovoltaic (PV), parabolic trough collector (PTC, with and without 6-hour thermal energy storage), linear Fresenel direct steam generation (dsg_lf), and solar hot water (swh). Scales generation to meet IPH demand based on estimates of available land area and system footprint.
* `pvhpmodel.py`: Translated from Matlab code developed by Steven Meyers to calculate tech opportunity of PV + ambient heat pump.
* `/calculation_data/`: Contains most data necessary for tech opportunity calculations. Other related, necessary data are in `/heat_load_calculations/calculation_data/`.
* `/archive/`: Contains code files not current in use.

## Technology Packages
Add table identifying the solar technology-IPH demand combinations, their relevant temperature ranges, industries, and other relevant info.
