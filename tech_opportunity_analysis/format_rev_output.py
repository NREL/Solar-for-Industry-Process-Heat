
import pandas as pd
import numpy as np
import h5py
import os

class rev_postprocessing:

    def __init__(self, rev_output_filepath, solar_tech):

        """
        Solar_tech is 'ptc_tes', 'ptc_notes', 'dsg_lf', 'pv', or 'swh'.
        """

        self.data_dir = './calculation_data/'

        gid_fips_file = 'county_center.csv'

        self.solar_tech = solar_tech

        # Crosswalk for matching FIPS to gid in 'meta' group.
        gid_to_fips = pd.read_csv(
            os.path.join(self.data_dir, gid_fips_file), usecols=['gid', 'FIPS']
            )

        gid_to_fips.rename(columns={'FIPS': 'COUNTY_FIPS'}, inplace=True)

        county_avail_area_file = 'county_rural_ten_percent_results_20200330.csv'

        # Import available county area (km2)
        self.area_avail = pd.read_csv(
            os.path.join(self.data_dir, county_avail_area_file),
            index_col=['County FIPS']
            )

        # Footprints in m2
        generation_groups = {
            'ptc_tes': {'power': ['q_dot_to_heat_sink'], # in MWt
                        'footprint': 16187},
            'dsg_lf': {'power':['q_dot_to_heat_sink'], # in MWt
                       'footprint':3698},
            'ptc_notes': {'power': ['q_dot_to_heat_sink'], # in MWt
                          'footprint':8094},
            'pv_ac': {'power': ['ac'], 'footprint':35208}, # 1-axis; in W
            'pv_dc': {'power':['dc'], 'footprint':42250}, # 1-axis; in W
            'swh': {'power': ['Q_deliv'], 'footprint':2024} # in kW
            }

        self.generation_group = generation_groups[self.solar_tech]

        # Read in file
        file = h5py.File(rev_output_filepath, 'r')

        county_info = pd.DataFrame(file['meta']['gid', 'timezone'],
                                   columns=['gid', 'timezone'])

        county_info['h5_index'] = county_info.index.values

        self.county_info = pd.merge(county_info, gid_to_fips, on=['gid'],
                                    how='left').set_index('COUNTY_FIPS')

        time_index = pd.DataFrame(file['time_index'], dtype=str)

        time_index = pd.to_datetime(time_index[0])

        # Use GHI as resource for all solar tech packages
        pv_output_filepath = 'c:/users/cmcmilla/desktop/rev_output/'+\
            'pv/pv_sc0_t0_or0_d0_gen_2014.h5'

        self.resource_h5file = h5py.File(pv_output_filepath, 'r')

        def resample_h5_dataset(h5py_file, dataset):
            """
            Resamples h5 generation dataset to hourly and convert from W or MW
            to kW. Also resamples resource dataset (in kW/m2).
            Returns dataframe with datetime index.
            """

            if dataset == 'resource':

                dataset_name = 'gh'

            else:

                dataset_name = generation_groups[self.solar_tech][dataset][0]

            resampled_df = pd.DataFrame(
                h5py_file[dataset_name][:,:], index=time_index
                )

            resampled_df = resampled_df.resample('H').sum()

            # PTC and thermal generation in MWt; SWH in kW
            # Solar resources for both in kW/m2.
            if (dataset_name=='q_dot_to_heat_sink'):

                resampled_df = resampled_df*1000

            # PV generation in W
            if (dataset=='power') & (self.solar_tech in ['pv_ac', 'pv_dc']):

                resampled_df = resampled_df/1000

            return resampled_df

        # Pull out array of generation (in kW) for all counties
        self.gen_kW = resample_h5_dataset(file, dataset='power')

        # Pull out solar resource (in kW/m2) for all counties
        self.resource = resample_h5_dataset(self.resource_h5file,
                                             dataset='resource')


    def scale_generation(self, county_fips, county_peak, month=1):
        """
        Scale generation based on month yield. Default is January (month=1).
        """


        gid, timezone, h5_index = self.county_info.xs(county_fips)[
            ['gid', 'timezone', 'h5_index']
            ]

        county_gen = pd.DataFrame(self.gen_kW.loc[:, h5_index])

        # Need to correct time based on each county timezone in
        # ['meta']['timezone'] using np.roll (only for gen)
        county_gen.iloc[:,0] = np.roll(county_gen, timezone)

        def get_county_gen_month(county_gen, county_fips):

            # calc January yield (kWh/kWp)
            county_gen_month = \
                county_gen.groupby(by=county_gen.index.month)[h5_index].agg(
                    ['sum', 'max']
                    )

            county_gen_month.rename(columns={'sum':'kWh', 'max':'kW_peak'},
                                   inplace=True)

            county_gen_month['yield'] = county_gen_month.kWh.divide(
                county_gen_month.kW_peak
                )

            return county_gen_month

        # Sum hourly resource (kW/m2) for annual kWh/m2
        # Convert to kWh/km2
        resource_annual = self.resource.sum()*1000**2

        county_gen_month = get_county_gen_month(county_gen, county_fips)

        month_yield = county_gen_month.xs(month)['yield']

        # Convert from kWh to MWh
        month_gen = county_gen_month.xs(month)['kWh']/1000

        area_avail = self.area_avail.xs(county_fips)[
            'County Available area km2'
            ]

        # Convert footprint from m2/MW to km2/MW
        footprint = self.generation_group['footprint']/1000**2

        # county_peak is in MWh (or MW).
        # (MWh/MW)/(km2/MW)*km2
        # rounds up
        if (np.ceil(area_avail/footprint)*month_gen) <= county_peak:

            scaled_gen = county_gen*np.ceil(area_avail/footprint)/1000

            used_area_abs = area_avail

        else:

            # round up for number of generating units
            scaled_gen = np.ceil(county_peak/month_yield)

            used_area_abs = scaled_gen*footprint

            scaled_gen = scaled_gen * county_gen/1000

        # There are 2 counties missing available land area
        if (area_avail == 0) | ([area_avail] == [np.nan]):

            used_area_pct = np.inf

        else:

            used_area_pct = used_area_abs / area_avail

        scaled_gen.index.name = 'index'

        scaled_gen.columns = ['MW']

        return scaled_gen, used_area_abs, used_area_pct

    # def plot_generation(self, county_generation):
    #     # Make
    #     # Select county my matching FIPS to self.gen_W column
    #     county_gen = pd.DataFrame(self.gen_W[:, 'county'])
    #
    #     # Need to correct time based on each county timezone in
    #     # ['meta']['timezone'] using np.roll (only for gen)
    #     county_gen.iloc[:,0] = np.roll(county_gen), timezone)



        # Make a quad chart (or three charts):
        # Topline (large) line plot of 8760 gen
        # Smaller plot of Avg monthly by peak, gen?
        # Single figure for avg hourly production for month, layering all 12 months.
        # Future tech potential plots by hourly and monthly net?





        # calcualte
        # in PV example:
        # gh is 30-minute global horizontal irradiance (W/m2)
        # ac/dc is 30-minute array power (W)
