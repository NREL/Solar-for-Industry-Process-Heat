import multiprocessing
import os
import make_load_curve
import pandas as pd
import logging
import numpy as np

class national_peak_load:
    """
    Peak load defined as MMBtu/hour.
    """

    def __init__(self, year):

        self.year = year

        self.county_energy = pd.read_parquet(
            'c:/users/cmcmilla/solar-for-industry-process-heat/results/'+\
            'mfg_eu_temps_20191031_2322.parquet.gzip'
            )

        # Import load shapes (defined by naics and employment size class)
        self.boiler_ls = pd.read_csv(
            'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
            'all_load_shapes_boiler.gzip', compression='gzip',
            index_col=['naics', 'Emp_Size']
            )

        self.ph_ls = pd.read_csv(
            'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
            'all_load_shapes_process_heat.gzip', compression='gzip',
            index_col=['naics', 'Emp_Size']
            )

        # ['COUNTY_FIPS', 'Emp_Size', 'MECS_FT', 'MECS_Region', 'data_source',
               # 'est_count', 'fipstate', 'naics', 'End_use', 'Temp_C', 'MMBtu'],

        # Calculate county energy total by industry, size, end use, and temperaure
        self.county_ind_size_temp_total = self.county_energy.groupby(
            ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'Temp_C']
            ).MMBtu.sum()

        # Calculate fuel mix by county, industry, size, and end use
        self.fuel_mix_enduse = self.county_energy.groupby(
            ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', 'MECS_FT']
            ).MMBtu.sum().divide(self.county_ind_size_temp_total.sum(level=[0,1,2,3]))


        # Calculate max load by county. Need to first aggregate energy by county,
        # naics, emp size, end use.
        self.boiler_energy_county_naics_emp = self.county_energy[
            (self.county_energy.End_use == 'CHP and/or Cogeneration Process') |
            (self.county_energy.End_use == 'Conventional Boiler Use')
            ].groupby(['COUNTY_FIPS', 'naics', 'Emp_Size']).MMBtu.sum()

        self.ph_energy_county_naics_emp = self.county_energy[
            self.county_energy.End_use == 'Process Heating'
            ].groupby(['COUNTY_FIPS', 'naics', 'Emp_Size']).MMBtu.sum()

        def make_blank_8760(year):

            dtindex = \
                pd.date_range(str(year)+'-01-01', str(year+1)+'-01-01', freq='H')[0:-1]

            load_8760_blank = pd.DataFrame(index=dtindex)

            load_8760_blank['month'] = load_8760_blank.index.month

            load_8760_blank['dayofweek'] = load_8760_blank.index.dayofweek

            load_8760_blank['Q'] = load_8760_blank.index.quarter

            load_8760_blank['hour'] = load_8760_blank.index.hour

            # load_8760_blank['date'] = load_8760_blank.index.date

            return load_8760_blank

        self.load_8760_blank = make_blank_8760(self.year)

    def calculate_8760_load(self, county, enduse):
        """
        Select county
        """

        if enduse == 'boiler':

            ph_or_boiler_energy_df = self.boiler_energy_county_naics_emp

            ph_or_boiler_ls = self.boiler_ls

        else:

            ph_or_boiler_energy_df = self.ph_energy_county_naics_emp

            ph_or_boiler_ls = self.ph_ls

        try:

            annual_energy = ph_or_boiler_energy_df.xs(county, level=0)

        except KeyError:

            return pd.DataFrame()

        load_8760 = pd.DataFrame(annual_energy).join(ph_or_boiler_ls)

        load_8760 = pd.merge(
            self.load_8760_blank.reset_index(), load_8760.reset_index(),
            on=['month', 'dayofweek', 'hour'], how='left'
            ).set_index(['naics', 'Emp_Size'])

        load_factor = load_8760[['Weekly_op_hours','Weekly_op_hours_low',
                                 'Weekly_op_hours_high']].mean(level=[0,1])

        # Determine peak demand using monthly load factor
        # Units are in power, not energy
        peak_demand = load_factor**-1

        peak_demand = peak_demand.multiply(annual_energy, axis=0)/8760

        load_8760.set_index('index', append=True, inplace=True)

        load_8760.sort_index(inplace=True)

        load_8760.update(
            load_8760[['Weekly_op_hours','Weekly_op_hours_low',
                       'Weekly_op_hours_high']].multiply(peak_demand)
            )

        load_8760.drop(['enduse', 'MMBtu', 'month', 'dayofweek', 'Q', 'hour'],
                       axis=1, inplace=True)

        return load_8760

    @staticmethod
    def find_peak_load(load_dfs_list):

        peak_load = pd.concat(load_dfs_list, axis=0)

        try:

            peak_load = peak_load.sum(level=2)

            peak_by_hrs_type = peak_load.max()

        except ValueError:

            peak_by_hrs_type = pd.DataFrame()

        # Is there a need to identify times when peak is met?
        # If so, do something like
        # for op_hrs in ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']:
        #   peak_load.where(peak_load.Weekly_op_hours == peak_by_hrs_type[op_hrs])[op_hrs].dropna()

        return peak_by_hrs_type

    def calculate_county_peak(self, county):

        print('County:', county)

        boiler_load_8760 = self.calculate_8760_load(county, 'boiler')

        ph_load_8760 =  self.calculate_8760_load(county, 'process heat')

        peak_load = national_peak_load.find_peak_load(
            [boiler_load_8760, ph_load_8760]
            )

        if len(peak_load.index) == 0:

            peak_load = 0

        peak_load =  pd.DataFrame.from_records(
            {county: peak_load},index=['Weekly_op_hours','Weekly_op_hours_low',
                                       'Weekly_op_hours_high'], columns=[county]
            )

        return peak_load

if __name__ == "__main__":

    __spec__ = None

    # set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('national_county_peaks.log', mode='w+')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    npls = national_peak_load(2014)
    logger.info('Peak load class instantiated.')

    # does this need to be a tuple?
    counties = npls.county_energy.COUNTY_FIPS.unique()

    county_peak_loads = pd.DataFrame()

    logger.info('starting multiprocessing')
    with multiprocessing.Pool(5) as pool:

        results = pool.map(npls.calculate_county_peak, counties)

        county_peak_loads = pd.concat(results, axis=1)

        #Convert from MMBtu/hr to MW
        county_peak_loads = county_peak_loads.multiply(0.293297)

    logger.info('Multiprocessing done')

    county_peak_loads.to_csv('../results/peak_load_by_county.csv')

    logger.info('Results saved done')
