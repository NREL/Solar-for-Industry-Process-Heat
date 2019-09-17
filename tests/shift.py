
import pandas as pd
import numpy as np

class schedule:

    def __init__(self, shift_length=8, weekly_op_hrs=40):

        self.shift_length = shift_length

        self.op_hours = weekly_op_hrs

    def calc_weekend_shift(self):
        """"
        Calculate number of daily weekend shifts based on
        shift length and weekly operating hours.
        """

        if self.op_hours < (self.shift_length*5):

            weekend_shift = 0

        elif self.op_hours < (self.shift_length*7):

            weekend_shift = (self.op_hours-self.shift_length*5)/2

        elif self.op_hours < (self.shift_length*(2*5+2)):

            weekend_shift = self.shift_length

        elif self.op_hours < (self.shift_length*2*7):

            weekend_shift = (self.op_hours-self.shift_length*2*5)/2

        elif self.op_hours < (24*5+self.shift_length*2*2):

            weekend_shift = self.shift_length*2

        else:

             weekend_shift = (self.op_hours-24*5)/2

        weekend_shift = weekend_shift / self.shift_length

        return weekend_shift


    def calc_shift_number_hours(self):
        """
        Calculate number of shifts per weekday.
        Also returns number of shifts per weekend day.
        """
        # Number of weekend operating hours
        weekend_shifts = self.calc_weekend_shift()

        daily_weekend_op_hours = self.shift_length*weekend_shifts

        daily_weekday_op_hours = \
            (self.op_hours - self.shift_length*weekend_shifts*2)/5

        weekday_shifts = daily_weekday_op_hours/self.shift_length

        shift_info = {'daily_weekday_op_hours': daily_weekday_op_hours,
                      'daily_weekend_op_hours': daily_weekend_op_hours,
                      'weekend_shifts': weekend_shifts,
                      'weekday_shifts': weekday_shifts}

        return shift_info

    def calc_shift_times(self, n_shifts, daily_op_hours):
        """

        """

        if n_shifts <= 1:

            start_time_1 = 9

        else:

            start_time_1 = 7

        if n_shifts < 1:

            end_time_1 = start_time_1+np.around(daily_op_hours)

        else:

            end_time_1 =  start_time_1+np.around(self.shift_length,0)

        if n_shifts <= 1:

            start_time_2 = False

            end_time_2 = False

        else:

            start_time_2 = end_time_1

            if n_shifts < 2:

                end_time_2 = start_time_2+np.around(
                    daily_op_hours-self.shift_length, 0
                    )

            else:

                end_time_2 = start_time_2+np.around(self.shift_length, 0)

        if n_shifts <= 2:

            start_time_3 = False

            end_time_3 = False

        else:

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

        return shift_times

    def calc_operating(self, weekday, hour, shift_times, shift_info):
        """

        """

        if weekday:

            n_shifts = shift_info['weekday_shifts']

            shift_hours = shift_info['daily_weekday_op_hours']

        else:

            n_shifts = shift_info['weekend_shifts']

            shift_hours = shift_info['daily_weekend_op_hours']

        if n_shifts <=1:

            if (hour>=shift_times['shift_1'][0]) & \
                (hour<=shift_times['shift_1'][1]):

                operating=True

            else:

                operating=False

        elif n_shifts <= 2:

            if (hour>=shift_times['shift_1'][0]) & \
                (hour<=shift_times['shift_2'][1]):

                operating=True

            else:

                operating=False

        elif shift_times['shift_3'][0] < shift_times['shift_1'][0]:

            if (hour>=shift_times['shift_3'][0]) & \
                (hour<=shift_times['shift_2'][1]):

                operating=True

            else:

                operating=False

        elif (hour>=shift_times['shift_3'][0]) | \
              (hour<=shift_times['shift_2'][1]):

              operating=True

        else:

            operating=False

        return operating

    def calc_weekly_schedule(self):
        """

        """

        # create week dataframe. Monday = 0
        week_sched = pd.DataFrame(
            np.vstack((np.repeat(range(0,7),24), np.tile(range(0,24),7))).T,
            columns=['dayofweek', 'hour']
            )

        week_sched['weekday'] = True

        week_sched.loc[week_sched[week_sched.dayofweek>4].index, 'weekday'] = \
            False

        shift_info = self.calc_shift_number_hours()

        shift_times = {
            True: self.calc_shift_times(
                n_shifts=shift_info['weekday_shifts'],
                daily_op_hours=shift_info['daily_weekday_op_hours']
                ),
            False: self.calc_shift_times(
                n_shifts=shift_info['weekend_shifts'],
                daily_op_hours=shift_info['daily_weekend_op_hours']
                )
            }

        week_sched['operating'] = week_sched.apply(
            lambda x: self.calc_operating(
                weekday=x['weekday'], hour=x['hour'],
                shift_times=shift_times[x['weekday']],
                shift_info=shift_info
                ), axis=1
            )

        return week_sched
