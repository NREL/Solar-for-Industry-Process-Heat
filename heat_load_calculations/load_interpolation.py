
import numpy as np
import pandas as pd

def interpolate_load(operating_schedule, peak_load, min_load):
    """
    Identifies the starting and ending hours of operation for a specific
    daytype. Also identifies the midpoint between the ending and beginning
    of operation. Linearly interpolates between ending and starting hours.
    """

    opsched = operating_schedule.copy(deep=True)

    # operating_schedule.set_index(['dayofweek', 'hour'], inplace=True)

    days = opsched.index.get_level_values(0).unique()

    for n in days:

        try:
            start = opsched.xs(n, level=0).where(
                opsched.xs(n, level=0) == True
                ).dropna().index[0]

        except IndexError:

            start, end, mid = np.repeat(np.nan,3)

        else:

            end = opsched.xs(n, level=0).where(
                opsched.xs(n, level=0) == True
                ).dropna().index[-1]

            mid = int(round(start/2, 0))

            if n > 0:

                try:

                    end_0 = opsched.xs(days[n-1], level=0).where(
                        opsched.xs(days[n-1], level=0) == True
                        ).dropna().index[-1]

                except IndexError:

                    pass

                else:

                    mid = int(round((start+(23-end_0))/2, 0))

        if np.isnan(start):

            data_points = opsched.xs(n).reset_index()

            data_points[opsched.name] = 'mid'

            data_points['dayofweek'] = n

            data_points.set_index(['dayofweek', 'hour'], inplace=True)

        else:

            data_points = pd.DataFrame(
                [[n, start, 'start'],[n, mid, 'mid'],[n, end, 'end']],
                columns=['dayofweek', 'hour', opsched.name]
                )

            data_points.set_index(['dayofweek', 'hour'], inplace=True)

        opsched.update(data_points[opsched.name])

    opsched.replace(
        {'start': peak_load, 'end':peak_load, 'mid':min_load, True: peak_load,
         False: np.nan}, inplace=True
        )

    # Use simple linear interpolation to fill NaNs.
    opsched.update(opsched.interpolate(limit_area='inside'))

    # Need to fill NaNs at beginning of week
    opsched.fillna(method='bfill', inplace=True)

    # Also need to fill any remaining times for Sunday
    sun_mid = np.round(
        (23-opsched.xs(6, level=0).dropna().index[-1])/2 + \
            opsched.xs(6, level=0).dropna().index[-1]
        )

    opsched.loc[(6,sun_mid)] = min_load

    opsched = opsched.interpolate()

    return opsched
