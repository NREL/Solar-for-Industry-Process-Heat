

import pandas as pd


def agg_county_by_region(county_energy_data):
    """

    """

    # split off energy calculated from MECS and group by Census Region (
    # i.e. MECS region)
    mecs_data = county_energy_data[county_energy_data.data_source='mecs_ipf']

    mecs_data = mecs_data.groupby(
        ['MECS_Region', 'naics', 'Emp_Size', 'MECS_FT', 'End_use', 'Temp_C']
        )[['MMBtu', 'est_count']].sum()

    mecs_data['MMBtu_intensity'] = mecs_data.MMBtu.divide(mecs_data.est_count)

    return mecs_data
