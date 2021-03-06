
import pandas as pd
import numpy as np

class schedule:

    def __init__(self, shift_length=8, weekly_op_hours=40):

        # Do not change shift length
        self.shift_length = shift_length

        self.op_hours = weekly_op_hours

        if self.op_hours > 168:

            self.op_hours = 168

    def calc_saturday_hours(self):
        """
        Calculate saturday operating hours based on weekly operating hours
        and shift length.
        """

        if self.op_hours <= (self.shift_length*2*5): # 80 for 8-hr shift

            saturday_hours = 0

        elif self.op_hours <= (self.shift_length*6*2): # 96 for 8-hr shift

            saturday_hours = self.op_hours - (self.shift_length*5*2)

        elif self.op_hours <= self.shift_length*(5*3+2*2): # 152 for 8-hr shift

            saturday_hours = self.shift_length*2

        elif self.op_hours <= 160:

            saturday_hours = self.op_hours - self.shift_length*(5*3+2)

        elif self.op_hours <=168:

            saturday_hours = self.shift_length*3

        return saturday_hours

    def calc_sunday_hours(self):
        """
        Calculate sunday operating hours based on saturday hours, weekly
        operating hours, and shift length.
        """
        if self.op_hours < (self.shift_length*5*2+8): # 96 for 8-hr shift

            sunday_hours = 0

        elif self.op_hours <= self.shift_length*7*2: # 112 for 8-hr shift

            sunday_hours = self.op_hours-(self.shift_length*6*2)

        elif self.op_hours <= 160:

            sunday_hours = self.shift_length*2

        elif self.op_hours <= 168:

            sunday_hours = self.op_hours-self.shift_length*6*3

        else:

            sunday_hours = 0

        return sunday_hours

    def calc_weekday_hours(self, saturday_hours, sunday_hours):
        """
        Calculate weekday operating hours based on weekly, saturday, and
        Sunday operating hours.
        """

        if saturday_hours == 24:

            weekday_hours = 24

        else:

            weekday_hours = (self.op_hours-saturday_hours-sunday_hours)/5

        return weekday_hours

    # def calc_weekend_shift(self):
    #     """"
    #     Calculate number of daily weekend shifts based on
    #     shift length and weekly operating hours.
    #     """
    #
    #     if self.op_hours < (self.shift_length*5):
    #
    #         weekend_shift = 0
    #
    #     elif self.op_hours < (self.shift_length*7):
    #
    #         weekend_shift = (self.op_hours-self.shift_length*5)/2
    #
    #     elif self.op_hours < (self.shift_length*(2*5+2)):
    #
    #         weekend_shift = self.shift_length
    #
    #     elif self.op_hours < (self.shift_length*2*7):
    #
    #         weekend_shift = (self.op_hours-self.shift_length*2*5)/2
    #
    #     elif self.op_hours < (24*5+self.shift_length*2*2):
    #
    #         weekend_shift = self.shift_length*2
    #
    #     else:
    #
    #          weekend_shift = (self.op_hours-24*5)/2
    #
    #     weekend_shift = weekend_shift / self.shift_length
    #
    #     return weekend_shift


    def calc_shift_number_hours(self):
        """
        Calculate number of shifts per weekday.
        Also returns number of shifts per weekend day.
        """

        # Calculate Sunday operating hours and number of shifts
        sunday_hours = self.calc_sunday_hours()

        sunday_shifts = sunday_hours/self.shift_length

        # Calculate Saturday operating hours and number of shifts
        saturday_hours = self.calc_saturday_hours()

        saturday_shifts = saturday_hours/self.shift_length

        # Calculate weekday operating hours and number of shifts
        daily_weekday_hours = self.calc_weekday_hours(saturday_hours,
                                                      sunday_hours)

        daily_weekday_shifts = daily_weekday_hours/self.shift_length

        shift_info = {'daily_weekday_hours': daily_weekday_hours,
                      'saturday_hours': saturday_hours,
                      'sunday_hours': sunday_hours,
                      'daily_weekday_shifts': daily_weekday_shifts,
                      'saturday_shifts': saturday_shifts,
                      'sunday_shifts': sunday_shifts}

        return shift_info

    def calc_shift_times(self, n_shifts, daily_op_hours):
        """

        """

        start_time_1 = 7

        end_time_1 = start_time_1+self.shift_length-1

        start_time_2 = end_time_1

        end_time_2 = start_time_2+np.around(
            daily_op_hours-self.shift_length, 0
            )

        start_time_3 = np.nan

        end_time_3 = np.nan

        if n_shifts == 0:

            shift_times = {'shift_1': (np.nan, np.nan),
                           'shift_2': (np.nan, np.nan),
                           'shift_3': (np.nan, np.nan)}

            return shift_times

        elif 0 < n_shifts <= 1:

            start_time_1 = 9

            start_time_2 = np.nan

            end_time_1 = start_time_1+self.shift_length-1

            end_time_2 = np.nan

        elif 2 < n_shifts < 3:

            start_time_3 = end_time_2

            end_time_3 = start_time_3+np.ceil(
                daily_op_hours - self.shift_length*2
                )

        elif n_shifts ==3:

            end_time_3 = start_time_1

            if np.around(daily_op_hours-self.shift_length,0) > start_time_1:

                start_time_3 = start_time_2

            else:

                start_time_3 = start_time_1-np.around(
                    daily_op_hours-2*self.shift_length, 0
                    )

        shift_times = {'shift_1': (start_time_1, end_time_1),
                       'shift_2': (start_time_2, end_time_2),
                       'shift_3': (start_time_3, end_time_3)}

        # Get rid of tuple nans
        for k in shift_times.keys():

            if shift_times[k] == (np.nan, np.nan):
                shift_times[k] = np.nan

        return shift_times

    # def calc_operating(self, dayofweek, hour, shift_times, shift_info):
    #     """
    #
    #     """

    def calc_operating(self, dayofweek, shift_times):

        shift_times_df = pd.DataFrame.from_dict(shift_times)

        if dayofweek in range(0,5):

            if shift_times_df[0].dropna().dtype == 'O':

                day_start = shift_times_df[0].dropna().min()[0]

                day_end = shift_times_df[0].dropna().max()[1]

            else:

                day_start = np.nan

                day_end = np.nan

        else:

            if shift_times_df[dayofweek].dropna().dtype == 'O':

                day_start = shift_times_df[dayofweek].dropna().min()[0]

                day_end = shift_times_df[dayofweek].dropna().max()[1]

            else:

                day_start = np.nan

                day_end = np.nan

        return day_start, day_end

        # if dayofweek in range(0,5):
        #
        #     n_shifts = shift_info['daily_weekday_shifts']
        #
        #     shift_hours = shift_info['daily_weekday_hours']
        #
        # elif dayofweek == 5:
        #
        #     n_shifts = shift_info['saturday_shifts']
        #
        #     shift_hours = shift_info['saturday_hours']
        #
        # else:
        #
        #     n_shifts = shift_info['sunday_shifts']
        #
        #     shift_hours = shift_info['sunday_hours']

        # if weekday:
        #
        #     n_shifts = shift_info['weekday_hourss']
        #
        #     shift_hours = shift_info['daily_weekday_op_hours']
        #
        # else:
        #
        #     n_shifts = shift_info['weekend_shifts']
        #
        #     shift_hours = shift_info['daily_weekend_op_hours']

        # if n_shifts == 0:
        #
        #     operating = False
        #
        # elif n_shifts<=1:
        #
        #     if (hour>=shift_times['shift_1'][0]) & \
        #         (hour<=shift_times['shift_1'][1]):
        #
        #         operating=True
        #
        #     else:
        #
        #         operating=False
        #
        # elif n_shifts <= 2:
        #
        #     if (hour>=shift_times['shift_1'][0]) & \
        #         (hour<=shift_times['shift_2'][1]):
        #
        #         operating=True
        #
        #     else:
        #
        #         operating=False
        #
        # elif n_shifts < 3:
        #
        #     if (hour>=shift_times['shift_2'][0]) & \
        #         (hour<=shift_times['shift_3'][1]):
        #
        #         operating=True
        #
        #     else:
        #
        #         operating=False
        #
        #
        #
        # elif shift_times['shift_3'][0] < shift_times['shift_1'][0]:
        #
        #     if (hour>=shift_times['shift_3'][0]) & \
        #         (hour<=shift_times['shift_2'][1]):
        #
        #         operating=True
        #
        #     else:
        #
        #         operating=False
        #
        # elif (hour>=shift_times['shift_3'][0]) | \
        #       (hour<=shift_times['shift_2'][1]):
        #
        #       operating=True
        #
        # else:
        #
        #     operating=False
        #
        # return operating

    def calc_weekly_schedule(self):
        """

        """

        # create week dataframe. Monday = 0
        week_sched = pd.DataFrame(
            np.vstack((np.repeat(range(0,7),24), np.tile(range(0,24),7))).T,
            columns=['dayofweek', 'hour']
            )

        shift_info = self.calc_shift_number_hours()

        shift_times = {
            0: self.calc_shift_times(
                n_shifts=shift_info['daily_weekday_shifts'],
                daily_op_hours=shift_info['daily_weekday_hours']
                ),
            5: self.calc_shift_times(
                n_shifts=shift_info['saturday_shifts'],
                daily_op_hours=shift_info['saturday_hours']
                ),
            6: self.calc_shift_times(
                n_shifts=shift_info['sunday_shifts'],
                daily_op_hours=shift_info['sunday_hours']
                )
            }

        new_week_sched = pd.DataFrame()

        week_sched['operating'] = np.nan

        week_sched.set_index(['dayofweek', 'hour'], inplace=True)

        for n in range(0, 7):

            day_start, day_end = self.calc_operating(n, shift_times)

            # Days without shifts turn True into 1. Skip these days.
            if np.isnan(day_start):

                continue

            operating_hours = \
                week_sched.xs(n, level=0).loc[day_start:day_end].fillna(True)

            operating_hours['dayofweek'] = n

            new_week_sched = new_week_sched.append(
                operating_hours, ignore_index=False
                )

        new_week_sched.reset_index(inplace=True)

        new_week_sched.set_index(['dayofweek','hour'], inplace=True)

        week_sched.operating.update(new_week_sched.operating)

        week_sched.reset_index(inplace=True)

        week_sched.operating.fillna(False, inplace=True)

        #
        # week_sched
#(3131, 'n250_499')
        # def apply_shift_times(week_sched, shift_times, shift_info):
        #
        #     try:
        #
        #         st = shift_times[week_sched['dayofweek']]
        #
        #     except KeyError:
        #
        #         st = shift_times[0]
        #
        #     operating = self.calc_operating(
        #         dayofweek=week_sched['dayofweek'], hour=week_sched['hour'],
        #         shift_times=st, shift_info=shift_info
        #         )
        #
        #     return operating
        #
        # week_sched['operating'] = week_sched.apply(
        #     lambda x: apply_shift_times(x, shift_times, shift_info), axis=1
        #     )

        # week_sched['operating'] = week_sched.apply(
        #     lambda x: self.calc_operating(
        #         weekday=x['weekday'], hour=x['hour'],
        #         shift_times=shift_times[x['weekday']],
        #         shift_info=shift_info
        #         ), axis=1
        #     )

        # print('weekday:', shift_info['daily_weekday_shifts'], '\n',
        #       'saturday:' ,shift_info['saturday_shifts'],'\n',
        #       'sunday:',shift_info['sunday_shifts'])

        daily_hours = pd.DataFrame(
            np.append(np.repeat(
                shift_info['daily_weekday_shifts']*self.shift_length, 5
                ), (shift_info['saturday_shifts']*self.shift_length,
             shift_info['sunday_shifts']*self.shift_length)),
            index=range(0,7), columns=['daily_hours']
            )

        if (shift_info['daily_weekday_shifts']<1.5) & (shift_info['saturday_shifts']==0):

            schedule_type = 'weekday_single'

        elif (shift_info['daily_weekday_shifts']>=1.5) & (shift_info['saturday_shifts']==0):

            schedule_type = 'weekday_double'

        elif (0<shift_info['saturday_shifts']<1.5):

            schedule_type = 'saturday_single'

        elif (1.5<=shift_info['saturday_shifts']<2):

            schedule_type = 'saturday_double'

        elif (0<shift_info['sunday_shifts']<1.5):

            schedule_type = 'sunday_single'

        elif (1.5<=shift_info['sunday_shifts']<2.5):

            schedule_type = 'sunday_double'

        else:

            schedule_type = 'continuous'

        return week_sched, schedule_type, daily_hours
