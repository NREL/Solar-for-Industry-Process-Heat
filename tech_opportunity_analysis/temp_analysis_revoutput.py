
import os
import pandas as pd
import numpy as np
import h5py
import seaborn as sns
import matplotlib.pyplot as plt

def analyze_temp(solar_tech):

    file_dir = 'c:/users/cmcmilla/desktop/rev_output/'
# 'dsg_lf': 'dsg_lf/dsg_lf_sc0_t0_or0_d0_gen_2014.h5',
    rev_files = {
                 'swh': 'swh/swh_sc0_t0_or0_d0_gen_2014.h5',
                 'ptc_tes': 'ptc_tes/ptc_tes6hr_sc0_t0_or0_d0_gen_2014.h5',
                 'ptc_notes': 'ptc_notes/ptc_notes_sc0_t0_or0_d0_gen_2014.h5'}

    temp_cols = {'swh': 'T_deliv', 'ptc_tes': 'T_heat_sink_in',
                 'ptc_notes': 'T_heat_sink_in'}

    # Read in h5 file
    rev_output = h5py.File(os.path.join(file_dir, rev_files[solar_tech]), 'r')

    t_col = temp_cols[solar_tech]

    time_index = pd.DataFrame(rev_output['time_index'], dtype=str)

    time_index = pd.to_datetime(time_index[0])

    temp_data = pd.DataFrame(rev_output[t_col], index=time_index)

    # Resample hourly as mean of 30-min data
    temp_data = temp_data.resample('H').mean()

    # Order each county by descending temp
    temp_data = pd.concat([pd.Series(
        temp_data[n].sort_values(ascending=False).values
        ) for n in temp_data.columns], axis=1, ignore_index=True)

    # Calculate mean temp over year by county
    temp_mean = temp_data.mean(axis=0).values

    temp_mean = temp_mean.reshape(1, len(temp_mean))

    temp_data = pd.DataFrame(np.vstack([temp_mean, temp_data.values]))

    min_mean = \
        temp_data.iloc[0][temp_data.iloc[0]==temp_data.iloc[0].min()].index[0]

    max_mean = \
        temp_data.iloc[0][temp_data.iloc[0]==temp_data.iloc[0].max()].index[0]

    final_data = pd.concat(
        [temp_data.iloc[1:, n] for n in [min_mean, max_mean]], axis=1
        )

    final_data.columns = ['min_mean_temp', 'max_mean_temp']

    final_data = pd.melt(final_data, var_name='min_or_max',
                         value_name='Temp_C')

    final_data.loc[:, 'hour'] = np.tile(range(0, 8760),2)

    final_data.loc[:, 'solar_tech'] = solar_tech

    return final_data

# Make all the data
package_temps = pd.concat(
    [analyze_temp(st) for st in ['swh', 'ptc_notes', 'ptc_tes']],
    axis=0, ignore_index=True
    )

# Plot the results
palette = dict(zip(['min_mean_temp', 'max_mean_temp'],
                   sns.color_palette("rocket_r", 6)))

# Plot the lines on two facets
sns.relplot(x='hour', y="Temp_C", hue="min_or_max", col="solar_tech",
            palette=palette, height=5, aspect=.75,
            facet_kws=dict(sharex=True, sharey=True), kind="line",
            legend="full", data=package_temps)
