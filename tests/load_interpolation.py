
def interpolate_load(operating_schedule, peak_load, turndown):
    """

    """

    min_load = peak_load / turndown

    def find_start_end(operating_schedule):
        """
        Identifies the starting and ending hours of operation for a specific
        daytype. Also identifies the midpoint between the ending and beginning
        of operation.
        """

        operating_schedule = operating_schedule.reset_index(drop=True)

        try:
            start = operating_schedule.operating.where(
                operating_schedule.operating == True
                ).dropna().index[0]

        except IndexError:

            return np.repeat(np.nan,3)

        else:

            end = operating_schedule.operating.where(
                operating_schedule.operating == True
                ).dropna().index[-1]

            mid = int(round((23 - end + start)/2, 0))

            if 0 < end < 17:

                mid = end + mid

            if (end >= 17) & (mid !=0):

                mid = end + mid - 24

            else:

                mid = np.nan

        return start, end, mid

    # Cacluate start, end, and midpoint for each daytype (weekday, Saturday,
    # Sunday)

    data_points = pd.concat(
        [pd.DataFrame(find_start_end(
            operating_schedule[operating_schedule.dayofweek == n]
            )).T for n in range(0,7)], axis=0, ignore_index=True
        )

    data_points = pd.concat([data_points, pd.Series(range(0,7))], axis=1)

    data_points.columns = ['start', 'end', 'mid', 'dayofweek']

    data_points = data_points.melt(id_vars='dayofweek', var_name='load',
                                   value_name='hour')

    data_points.dropna(inplace=True)

    data_points.replace({'start': peak_load, 'end':peak_load, 'mid':min_load},
                        inplace=True)

    operating_schedule = \
        operating_schedule.set_index(['dayofweek', 'hour']).join(
            data_points.set_index(['dayofweek', 'hour'])
            )

    # Fill between start and end of operations
    operating_schedule.update(
        operating_schedule[operating_schedule.operating==True].fillna(
            method='ffill'
            )
        )

    # Fill in days with no operating hours
    for n in range(0, 7):

        if not any(operating_schedule.loc[(n,), 'operating']):

            operating_schedule.loc[(n,), 'load'] = min_load

        else:

            continue







    operating_schedule['load'] = np.nan

    operating_schedule

    # Must fix very beginning and ending of week.
