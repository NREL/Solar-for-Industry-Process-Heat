
import pandas as pd


class demand_hourly_load:
    """

    """

    def __init__(self, year, county_filepath_or_dataframe):

        #County_energy_filepath:
        #'c:/users/cmcmilla/solar-for-industry-process-heat/results/mfg_eu_temps_20191031_2322.parquet.gzip'
        self.year = year

        # self.results_dir = \
        #     'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +\
        #     'national_loads_8760/'

        if type(county_filepath_or_dataframe) == pd.core.frame.DataFrame:
            county_energy = county_filepath_or_dataframe

        elif type(county_filepath_or_dataframe) == str:
            if '.csv' in county_filepath_or_dataframe:
                county_energy = pd.read_csv(county_filepath_or_dataframe)

            if '.parquet' in county_filepath_or_dataframe:
                county_energy = pd.read_parquet(
                    county_filepath_or_dataframe
                    )

        else:

            raise(TypeError)

        self.county_energy = county_energy

        # Import load shapes (defined by naics and employment size class)
        self.boiler_ls = pd.read_csv(
            'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +
            'all_load_shapes_boiler_20200728.gzip', compression='gzip',
            index_col=['naics', 'Emp_Size']
            )
        self.ph_ls = pd.read_csv(
            'c:/users/cmcmilla/solar-for-industry-process-heat/results/' +
            'all_load_shapes_process_heat_20200728.gzip', compression='gzip',
            index_col=['naics', 'Emp_Size']
            )

        def condense_by_quarter(load_shape):
            """
            Condenses load shapes down by quarter because they only change
            from quarter to quarter.
            """
            load_shape['Q'] = \
                pd.cut(load_shape.month, 4, labels=[1, 2, 3, 4])
            load_shape = load_shape.reset_index().drop_duplicates(
                ['naics', 'Emp_Size', 'Q', 'dayofweek', 'hour']
                )
            load_shape.set_index(['naics', 'Emp_Size'], inplace=True)

            return load_shape

        self.boiler_ls = condense_by_quarter(self.boiler_ls)
        self.ph_ls = condense_by_quarter(self.ph_ls)

        def calc_data_aggs(county_energy, size_class, column):
            """
            Calculate temperature fraction by county, naics, employment size,
            and end use. Split approach into data calcualted from ghgrp and
            from MECS.

            Parameters
            ----------
            county_energy : dataframe

            size_class : str

            column : str

            Returns
            -------
            agg : dataframe

            """

            if size_class == 'ghgrp':
                agg = \
                    county_energy[county_energy.Emp_Size == size_class].groupby(
                        ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', column]
                        ).MMBtu.sum()

            else:
                agg = \
                    county_energy[county_energy.Emp_Size != 'ghgrp'].groupby(
                        ['COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use', column]
                        ).MMBtu.sum()

            agg = agg.divide(agg.sum(level=[0, 1, 2, 3]))

            return agg

        def make_blank_8760(year):
            """
            Make a dataframe for a given year, including columns for month,
            dayofweek, quarter, and hour.
            """
            dtindex = pd.date_range(
                str(year)+'-01-01', str(year+1)+'-01-01', freq='H'
                )[0:-1]
            load_8760_blank = pd.DataFrame(index=dtindex)
            load_8760_blank['month'] = load_8760_blank.index.month
            load_8760_blank['dayofweek'] = load_8760_blank.index.dayofweek
            load_8760_blank['Q'] = load_8760_blank.index.quarter
            load_8760_blank['hour'] = load_8760_blank.index.hour

            return load_8760_blank

        self.load_8760_blank = make_blank_8760(self.year)

    # @staticmethod
    # def save_8760_load_parquet(county_load_8760, county, results_dir):
    #     """
    #     Method for saving county 8760 load to a parquet file.
    #     """
    #     file_name = 'county_'+str(county)+'.parquet'
    #
    #     if file_name in os.listdir(results_dir):
    #
    #             shutil.rmtree(os.path.join(results_dir,file_name))
    #
    #     if type(county_load_8760) == pd.core.frame.DataFrame:
    #
    #         county_load_8760 = pd.DataFrame(county_load_8760).reset_index()
    #
    #         county_load_8760.to_parquet(
    #             os.path.join(results_dir, file_name), engine='pyarrow',
    #             partition_cols=['op_hours'],compression='snappy', index=None
    #             )
    #
    #     else:
    #
    #         county_load_8760.to_parquet(
    #             os.path.join(results_dir, file_name), engine='pyarrow',
    #             partition_on=['op_hours'],compression='snappy',
    #             write_index=True, compute=True
    #             )
    #
    #     # Check if file saved
    #     if file_name in os.listdir(results_dir):
    #
    #         print('Results for {} saved.'.format(county))
    #
    #     else:
    #         raise OSError ('File {} NOT saved.'.format(file_name))

    def calculate_8760_load(self, county, enduse):
        """
        Calculate hourly load for a single county for a single end use type.

        Parameters
        ----------
        county : int
            FIPS code of county

        enduse : {'boier', 'process heat'}
            End use type for representative load shapes by NAICS and
            employment size class.

        Returns
        -------
        load_8760 : dataframe
            Hourly load in MW, indexed by enduse, timestamp, and operating
            hours category (mean, high, and low).
        """

        annual_energy = self.county_energy[
            self.county_energy.COUNTY_FIPS == county
            ].copy(deep=True)

        if annual_energy.empty:
            return pd.DataFrame()

        else:
            if enduse == 'boiler':
                ph_or_boiler_energy_df = annual_energy[
                    (annual_energy.End_use != 'Process Heating')
                    ].groupby(['COUNTY_FIPS', 'naics', 'Emp_Size',
                               'End_use']).MMBtu.sum()

                ph_or_boiler_ls = self.boiler_ls

            if enduse == 'process heat':
                ph_or_boiler_energy_df = annual_energy[
                    (annual_energy.End_use == 'Process Heating')
                    ].groupby(['COUNTY_FIPS', 'naics', 'Emp_Size',
                               'End_use']).MMBtu.sum()

                ph_or_boiler_ls = self.ph_ls

        if ph_or_boiler_energy_df.empty:
            return pd.DataFrame()

        # Not all counties have boiler and process heating loads.
        # Return an empty dataframe
        try:
            ph_or_boiler_energy_df.reset_index(['COUNTY_FIPS'], drop=True,
                                               inplace=True)

        except:
            print('EXCEPTION')
            return pd.DataFrame()

        else:
            try:
                load_8760 = pd.DataFrame(ph_or_boiler_energy_df).join(
                    ph_or_boiler_ls
                    )

            except IndexError as e:
                print(e)
                return pd.DataFrame()

            else:
                # Currently merges on quarter because load shapes only change by
                # quarter, dayofweek, and hour by NAICS-emp_size pair.
                load_8760 = pd.merge(
                    self.load_8760_blank.reset_index(), load_8760.reset_index(),
                    on=['Q', 'dayofweek', 'hour'], how='left'
                    ).set_index(['naics', 'Emp_Size'])

                load_factor = load_8760[
                    ['Weekly_op_hours', 'Weekly_op_hours_low',
                     'Weekly_op_hours_high']
                     ].mean(level=[0, 1])

        # Determine peak demand using monthly load factor
        # Units are in power, not energy
        peak_demand = load_factor**-1

        try:
            peak_demand = peak_demand.multiply(ph_or_boiler_energy_df,
                                               axis=0)/8760
        # Process heat enduses are throwing assertion errors for an unknown
        # reason. Traceback as follows:
        # ~\AppData\Local\Continuum\anaconda3\lib\site-packages\pandas\core\frame.py in _combine_match_index(self, other, func, level)
        #    5096         left, right = self.align(other, join='outer', axis=0, level=level,
        #    5097                                  copy=False)
        # -> 5098         assert left.index.equals(right.index)
        except AssertionError:
            peak_demand = peak_demand.join(ph_or_boiler_energy_df)
            peak_demand = peak_demand.multiply(peak_demand.MMBtu, axis=0)/8760

        load_8760.set_index(['End_use', 'index'], append=True, inplace=True)
        load_8760.sort_index(inplace=True)

        load_8760.update(
            load_8760[['Weekly_op_hours', 'Weekly_op_hours_low',
                       'Weekly_op_hours_high']].multiply(peak_demand)
            )

        load_8760.rename(columns={'Weekly_op_hours': 'ophours_mean',
                                  'Weekly_op_hours_low': 'ophours_low',
                                  'Weekly_op_hours_high': 'ophours_high'},
                         inplace=True)

        load_8760.drop(['MMBtu', 'month_x', 'month_y', 'dayofweek', 'Q',
                        'hour', 'enduse'], axis=1, inplace=True)

        # Melt data
        load_8760 = load_8760.reset_index().melt(
            id_vars=load_8760.index.names, var_name='op_hours',
            value_vars=['ophours_mean', 'ophours_low', 'ophours_high'],
            value_name='load_MMBtu_per_hour'
            ).set_index(load_8760.index.names)

        # Convert from MMBtu/hour to MW
        load_8760['MW'] = \
            load_8760.load_MMBtu_per_hour.multiply(0.293297)

        load_8760['MW'] = load_8760.MW.astype('float32')
        load_8760.drop(['load_MMBtu_per_hour'], axis=1, inplace=True)
        load_8760.set_index('op_hours', append=True, inplace=True)

        return load_8760


    # # Not used
    # def calculate_ft_temp(self, county, load_8760, enduse):
    #     """
    #     Break out calculations by fuel type and temperature.
    #     Method handles Harris County, TX (fips = 48201) differently due to
    #     data size (pandas operations result in memory allocation errors)
    #     """
    #
    #     # Harris county (fips 48201) results in a very large dataframe and
    #     # memory allocation errors with pandas methods.
    #     if county == 48201:
    #
    #         # Setting npartions
    #         load_8760 = dd.from_pandas(load_8760.reset_index(), npartitions=50)
    #
    #         temp_eu_data = self.temp_fraction.xs(county).multiply(
    #             self.fuel_mix_enduse.xs(county)
    #             )
    #
    #         def temp_fuel_mult(x, temp_eu_data):
    #
    #             x = x.set_index(
    #                 ['naics', 'Emp_Size','End_use']
    #                 ).join(temp_eu_data)
    #
    #             x.load_MMBtu_per_hour.update(
    #                 x.load_MMBtu_per_hour.multiply(x.MMBtu)
    #                 )
    #
    #             x['load_MMBtu_per_hour'] = \
    #                 x.load_MMBtu_per_hour.astype('float32')
    #
    #             return x
    #
    #         temp_load_8760 = load_8760.map_partitions(
    #             lambda x: temp_fuel_mult(x, temp_eu_data)
    #             )
    #
    #         temp_load_8760 = temp_load_8760.reset_index()
    #
    #         temp_load_8760 = temp_load_8760.drop(columns=['MMBtu'])
    #
    #     else:
    #         # Multiply by temperature fraction and fuel mix
    #         temp_load_8760 = load_8760.load_MMBtu_per_hour.multiply(
    #             self.temp_fraction.xs(county).multiply(
    #                 self.fuel_mix_enduse.xs(county)
    #                 )
    #             )
    #
    #         temp_load_8760 = temp_load_8760.dropna()
    #
    #         temp_load_8760.name = 'load_MMBtu_per_hour'
    #
    #         temp_load_8760['load_MMBtu_per_hour'] = \
    #             temp_load_8760.load_MMBtu_per_hour.astype('float32')
    #
    #     # Save results as parquet files
    #     save_8760_load(temp_load_8760, county, self.results_dir)
    #
    #     return

    @staticmethod
    def find_peak_load(load_8760, month, power=False):
        """
        Specify month as sigle integer (1-12) or use 'all' to find peak load
        for year.
        Peak is defined as either total energy for selected month or by
        peak hour MW (power = True).
        """
        # peak_load = pd.concat(load_8760_dfs, axis=0, ignore_index=False,
        #                       sort_index=True)

        peak_load = load_8760.copy(deep=True)

        peak_load.reset_index(['naics', 'Emp_Size', 'End_use'], drop=True,
                              inplace=True)

        peak_load.reset_index(['op_hours'], drop=False, inplace=True)

        if month != 'all':
            peak_load = peak_load[peak_load.index.month == month]

        if power:
            peak_load = peak_load.groupby([peak_load.index, 'op_hours']).sum()
            peak_by_hrs_type = peak_load.max(level=1)

        else:
            peak_load = peak_load.groupby(['op_hours']).sum()
            peak_by_hrs_type = peak_load
            peak_by_hrs_type.columns = ['MWh']

        return peak_by_hrs_type

    def calculate_county_8760_and_peak(self, county, peak_month=12,
                                       peak_MW=False):

        eu_load_8760 = pd.concat(
            [self.calculate_8760_load(county, eu) for eu in ['boiler',
                                                             'process heat']],
            axis=0, ignore_index=False, sort=True
            )

        peak_load = self.find_peak_load(eu_load_8760, month=peak_month,
                                        power=peak_MW)

        peak_load.columns = [str(county)+'_' + peak_load.columns[0]]

        return eu_load_8760, peak_load
