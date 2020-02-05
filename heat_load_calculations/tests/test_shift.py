
import pytest
import shift
import numpy as np
import pandas as pd

def test_shift():
    """
    First assertion tests that the total weekly operating hours post algorithm
    equal the user-specified total weekly operating hours.

    Second assertion tests that the weekly operating schedule (denoted by
    True or False) results in the correct number of daily operting hours.
    """

    test_hours = 113

    shift_test = shift.schedule(weekly_op_hours=test_hours)

    week_sched, schedule_type, daily_hours = shift_test.calc_weekly_schedule()

    hours_sum = daily_hours.sum()[0]

    hrs_per_operating = pd.concat([daily_hours,week_sched.where(
            week_sched.operating==True
            ).groupby('dayofweek').operating.sum()], axis=1
        )

    hrs_per_operating.daily_hours.update(
        hrs_per_operating.daily_hours.divide(
            hrs_per_operating.operating
            )
        )

    # hrs_per_operating = daily_hours.apply(
    #     lambda x: x.divide(np.floor(x.divide(8))*8)
    #     )

    weekly_hours_sum = week_sched.set_index('dayofweek').join(
        hrs_per_operating['daily_hours']
        )

    weekly_hours_sum = weekly_hours_sum.daily_hours.multiply(
        weekly_hours_sum.operating
        ).sum(level=0)

    assert test_hours == hours_sum

    # This tests that the correct number of hours are designated as operating
    assert all(weekly_hours_sum == daily_hours.daily_hours)
