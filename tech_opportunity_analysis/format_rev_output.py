
import pandas as pd
import h5py
import os

class rev_postprocessing:

    def __init__(self, rev_output_filepath, solar_tech):

        self.data_dir = './calculation_data/'

        gid_fips_file = 'county_center.csv'

        self.solar_tech = solar_tech

        # Crosswalk for matching FIPS to gid in 'meta' group.
        gid_to_fips = pd.read_csv(
            os.path.join(self.data_dir, gid_fips_file), usecols=['gid', 'FIPS']
            )

        gid_to_fips.rename(columns={'FIPS': 'COUNTY_FIPS'}, inplace=True)

        rev_output_filepath = \
            'c:/users/cmcmilla/desktop/rev_output/ptc_tes/ptc_tes6hr_sc0_t0_or0_d0_gen_2014.h5'

        generation_groups = {
            'ptc_tes': {'power': ['q_dot_to_heat_sink'],
                        'resource': ['']},
            'dsg_lf': {'power':['q_dot_to_heat_sink'],
                       'resource': []},
            'ptc_notes': {'power': ['q_dot_to_heat_sink'],
                          'resource': ['beam']},
            'pv': {'power_ac': ['ac'], 'power_dc':['dc'],
                   'resource': ['gh']'},
            'swh': {'power': ['Q_deliv'], 'temp': ['T_deliv'],
                    'resource': ['']}
            }
        # other? ptc_tes: annual_energy (kWt-hr);

        self.default_pkgparams = {
            'dsg_lf': {'industries': , 'temp_range':[0,], 'end_uses':},
            'ptc_notes': {'industries': , 'temp_range':[], 'end_uses': },
            'ptc_tes': {'industries': , 'temp_range':[], 'end_uses': },
            'pv:' {'industries': , 'temp_range':[], 'end_uses': },
            'swh': {'industries': , 'temp_range':[], 'end_uses': }
            }
        # Read in file
        file = h5py.File(rev_output_filepath, 'r')

        county_info = pd.DataFrame(file['meta']['gid', 'timezone'],
                                   columns=['gid', 'timezone'])

        county_info['h5_index'] = county_info.index.values

        self.county_info = pd.merge(county_info, gid_to_fips, on=['gid'],
                                    how='left').set_index('COUNTY_FIPS')

        time_index = pd.DataFrame(file['time_index'], dtype=str)

        time_index = pd.to_datetime(time_index[0])

        def resample_h5_dataset(h5py_file, dataset):
            """
            Resamples h5 generation dataset to hourly and convert from W or MW
            to kW. Also resamples resource dataset (in kW/m2).
            Returns dataframe with datetime index.
            """

            dataset_name = \
                h5py_file[generation_group[self.solar_tech][dataset].name

            resampled_df = pd.DataFrame(
                h5py_file[dataset_name][:,:], index=time_index
                )

            resampled_df = resampled_df.resample('H').sum()

            # PV generation in W; solar thermal generation in MW
            # Solar resources for both in kW/m2.
            if ((dataset=='power') & (dataset_name=='/q_dot_to_heat_sink') | \
                (dataset_name == '/Q_deliv')):

                resampled_df = resampled_df*1000

            elif (dataset=='power'):

                resampled_df = resampled_df/1000

            else:

                pass

            return resampled_df

        # Pull out array of generation (in kW) for all counties
        self.gen_kW = resample_h5_dataset(file, dataset='power')

        # Pull out solar resource (in kW/m2) for all counties
        self.resource = resampled_h5_dataset(file, dataset='resource')

    def get_county_gen_month(self, county_fips):

        gid, timezone, h5_index = self.county_info.xs(county_fips)[
            ['gid', 'timezone', 'h5_index']
            ]

        # Make
        # Select county my matching FIPS to self.gen_W column
        county_gen = pd.DataFrame(self.gen_kW[:, h5_index])

        # Need to correct time based on each county timezone in
        # ['meta']['timezone'] using np.roll (only for gen)
        county_gen.iloc[:,0] = np.roll(county_gen), timezone)

        # calc January yield (kWh/kWp)
        county_gen_month = \
            gen_kW_county.groupby(by=gen_kw_county.index.month).agg(
                ['sum', 'max']
                )

        county_gen_month.rename(columns={'sum':'kWh', 'max':'kW_peak'},
                               inplace=True)

        county_gen_month['yield'] = county_gen_month.kWh.divide(
            county_gen_month.kW_peak
            )

        return county_gen_month


    def plot_generation(self, county_generation):
        # Make
        # Select county my matching FIPS to self.gen_W column
        county_gen = pd.DataFrame(self.gen_W[:, 'county'])

        # Need to correct time based on each county timezone in
        # ['meta']['timezone'] using np.roll (only for gen)
        county_gen.iloc[:,0] = np.roll(county_gen), timezone)



        # Make a quad chart (or three charts):
        # Topline (large) line plot of 8760 gen
        # Smaller plot of Avg monthly by peak, gen?
        # Single figure for avg hourly production for month, layering all 12 months.
        # Future tech potential plots by hourly and monthly net?



    def calc_():


        # calcualte
        # in PV example:
        # gh is 30-minute global horizontal irradiance (W/m2)
        # ac/dc is 30-minute array power (W)
