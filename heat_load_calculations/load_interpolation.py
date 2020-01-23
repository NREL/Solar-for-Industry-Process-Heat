
def interpolate_load(operating_schedule, peak_load, turndown):
    """
    Identifies the starting and ending hours of operation for a specific
    daytype. Also identifies the midpoint between the ending and beginning
    of operation. Linearly interpolates between ending and starting hours.
    """

    min_load = peak_load / turndown

    operating_schedule.set_index(['dayofweek', 'hour'], inplace=True)

    days = operating_schedule.index.get_level_values(0).unique()

    for n in days:

        try:
            start = operating_schedule.xs(n, level=0).operating.where(
                operating_schedule.xs(n, level=0).operating == True
                ).dropna().index[0]

        except IndexError:

            start, end, mid = np.repeat(np.nan,3)

        else:

            end = operating_schedule.xs(n, level=0).operating.where(
                operating_schedule.xs(n, level=0).operating == True
                ).dropna().index[-1]

            try:

                end_0 = operating_schedule.xs(days[n-1], level=0).operating.where(
                    operating_schedule.xs(days[n-1], level=0).operating == True
                    ).dropna().index[-1]

            except IndexError:

                mid = int(round((start-0)/2, 0))

            else:

                mid = int(round((start+(24-end_0))/2, 0))

        if np.isnan(start):

            data_points = operating_schedule.xs(n).reset_index()

            data_points['operating'] = 'mid'

            data_points['dayofweek'] = n

            data_points.set_index(['dayofweek', 'hour'], inplace=True)

        else:

            data_points = pd.DataFrame(
                [[n, start, 'start'],[n, mid, 'mid'],[n, end, 'end']],
                columns=['dayofweek', 'hour', 'operating']
                )

            data_points.set_index(['dayofweek', 'hour'], inplace=True)

        operating_schedule.update(data_points)

    operating_schedule.replace(
        {'start': peak_load, 'end':peak_load, 'mid':min_load, True: peak_load,
         False: np.nan}, inplace=True
        )

    # Use simple linear interpolation to fill NaNs.
    operating_schedule.update(operating_schedule.interpolate())

    # Need to fill NaNs at beginning of week
    operating_schedule.fillna(method='bfill', inplace=True)

    return operating_schedule
