import pandas as pd
import os
import numpy as np

file_dir = 'c:/Users/cmcmilla/desktop/load curves'

load_shapes = pd.DataFrame()

blank = pd.DataFrame(columns=['sic', 'weekday', 'saturday', 'sunday'],
                           index=range(1, 25))

for file in os.listdir(file_dir):

    if file == 'EPRI_loadshape_data.csv':

        continue

    shape = pd.read_csv(file_dir+'/'+file, index_col='x')

    shape = shape.append(blank, sort=True)

    shape.sort_index(inplace=True)

    sic = file.split('_')[1].split('.')[0]

    shape['sic'] = sic

    shape.interpolate(method='linear', inplace=True, limit_direction='both')

    shape = shape[~shape.index.duplicated()]

    load_shapes = load_shapes.append(shape, ignore_index=False, sort=True)

for day in ['weekday', 'saturday', 'sunday']:

    load_shapes.loc[:, day] = np.around(load_shapes[day], 3)

load_shapes = load_shapes.loc[range(1, 25), :]

load_shapes.index.name = 'hour'

load_shapes.reset_index(inplace=True)

# Combine with other dataset
other_shapes = pd.read_csv(file_dir+'/'+'EPRI_loadshape_data.csv')

other_shapes = other_shapes.melt(id_vars=['SIC', 'day_type'], var_name='hour')

other_shapes = other_shapes.pivot_table(
    index=['SIC', 'hour'], columns=['day_type'], aggfunc='mean'
    ).reset_index()

other_shapes.columns=['sic', 'hour', 'saturday', 'sunday', 'weekday']

load_shapes = load_shapes.append(other_shapes, ignore_index=True, sort=False)

load_shapes.sort_values(by=['sic', 'hour'], inplace=True)

load_shapes.to_csv('c:/users/cmcmilla/desktop/epri_shapes.csv', index=False)
