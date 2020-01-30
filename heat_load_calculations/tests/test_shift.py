
import pytest
import shift

def test_shift():
    """
    First assertion tests that the total weekly operating hours post algorithm
    equal the user-specified total weekly operating hours.

    Second assertion tests that the weekly operating schedule (denoted by
    True or False) results in the correct number of daily operting hours.
    """

    test_hours = 42

    shift_test = shift.schedule(weekly_op_hours=test_hours)

    week_sched, schedule_type, daily_hours = shift_test.calc_weekly_schedule()

    hours_sum = daily_hours.sum()[0]

    hrs_per_operating = daily_hours/8

    weekly_hours_sum = week_sched.set_index('dayofweek').join(
        hrs_per_operating
        )

    weekly_hours_sum = weekly_hours_sum.daily_hours.multiply(
        weekly_hours_sum.operating
        ).sum(level=0)

    assert test_hours == hours_sum

    assert all(weekly_hours_sum == daily_hours.daily_hours)
