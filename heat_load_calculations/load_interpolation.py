
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

        print(n)

        try:
            start = opsched.xs(n, level=0).where(
                opsched.xs(n, level=0) == True
                ).dropna().index[0]

        except IndexError:

            start, end, startup, shutdown = np.repeat(np.nan, 4)

            # start, end, mid = np.repeat(np.nan,3)

        else:

            end = opsched.xs(n, level=0).where(
                opsched.xs(n, level=0) == True
                ).dropna().index[-1]

            # Mid sets the starting point of interpolation (from mid to start)
            # for morning start-up.
            # mid = start - 3
            # mid = int(round(start/2, 0))
            # Start up sets the time where unit is operating at overnight load
            # and begins ramping to operating load
            startup = start - 3

            if end <= 20:

                shutdown = end + 3

            else:

                shutdown = np.nan

            if n > 0:

                try:

                    end_0 = opsched.xs(days[n-1], level=0).where(
                        opsched.xs(days[n-1], level=0) == True
                        ).dropna().index[-1]

                except IndexError:

                    pass

                else:

                    shutdown = range(end_0+3-23, startup)
                    # mid = int(round((start+(23-end_0))/2, 0))

        if np.isnan(start):

            data_points = opsched.xs(n).reset_index()

            data_points[opsched.name] = 'shutdown'

            data_points['dayofweek'] = n

            data_points.set_index(['dayofweek', 'hour'], inplace=True)

        else:

            if type(shutdown) == range:

                data_points = pd.DataFrame([
                    [n,startup,'startup'], [n,start,'start'], [n,end,'end'],
                    ], columns=['dayofweek', 'hour', opsched.name])

                dp_shutdown = pd.DataFrame(np.vstack(
                    [np.repeat(n, len(shutdown)), shutdown,
                     np.repeat('shutdown', len(shutdown))]
                     ).T,columns=['dayofweek', 'hour', opsched.name])

                # np.vstack is turning integers to strings for some reason.
                dp_shutdown = dp_shutdown.astype({'dayofweek':int, 'hour':int})

                data_points = data_points.append(dp_shutdown)

            else:

                data_points = pd.DataFrame(
                    [[n,startup,'startup'], [n,start,'start'], [n,end,'end'],
                     [n,shutdown,'shutdown']],
                    columns=['dayofweek', 'hour', opsched.name]
                    )

            # Check if following day has any operating hours. If not, make sure
            # shut down is happening in current day.

            if n < 6:

                if (all(opsched.xs(n+1, level=0)) == False) & (end+3 <= 23):

                    dp_shutdown = pd.DataFrame(np.vstack(
                        [np.repeat(n, 23-(end+2)), range(end+3, 23+1),
                         np.repeat('shutdown', 23-(end+2))]
                         ).T,columns=['dayofweek', 'hour', opsched.name])

                    # np.vstack is turning integers to strings for some reason.
                    dp_shutdown = dp_shutdown.astype(
                        {'dayofweek':int, 'hour':int}
                        )

                    data_points = data_points.append(dp_shutdown)

            # data_points = pd.DataFrame(
            #     [[n, start, 'start'],[n, mid, 'mid'],[n, end, 'end']],
            #     columns=['dayofweek', 'hour', opsched.name]
            #     )

            data_points = data_points.dropna().set_index(
                ['dayofweek', 'hour']
                ).sort_index()

            # Drop cases where shutdown time entry is duplicated
            data_points = data_points[~data_points.index.duplicated()]

        opsched.update(data_points[opsched.name])

    opsched.replace(
        {'start': peak_load, 'end':peak_load, 'startup':min_load,
         'shutdown': min_load, True: peak_load, False: np.nan}, inplace=True
        )

    # Use simple linear interpolation to fill NaNs.
    opsched.update(opsched.interpolate(limit_area='inside'))

    # Need to fill NaNs at beginning of week
    opsched.fillna(method='bfill', inplace=True)

    # Also need to fill any remaining times for Sunday
    if (23-opsched.xs(6, level=0).dropna().index[-1] > 3):

        sun_shutdown = range(opsched.xs(6, level=0).dropna().index[-1]+3, 24)

        opsched.loc[(6, [x for x in sun_shutdown])] = min_load

    else:

        sun_shutdown = 23

        opsched.loc[(6, sun_shutdown)] = min_load

    # sun_shutdown = np.round(
    #     (23-opsched.xs(6, level=0).dropna().index[-1])/2 + \
    #         opsched.xs(6, level=0).dropna().index[-1]
    #     )

    opsched = opsched.interpolate()

    return opsched
