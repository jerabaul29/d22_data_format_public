import datetime
import pytz

from d22_data_format.helpers.raise_assert import ras

def assert_is_utc_datetime(date_in):
    """Assert that date_in is an UTC datetime."""
    ras(isinstance(date_in, datetime.datetime))

    if not date_in.tzinfo == pytz.utc:
        raise Exception("not utc!")

def datetime_range(datetime_start, datetime_end, step_timedelta):
    """Yield a datetime range, in the range [datetime_start; datetime_end[,
    with step step_timedelta."""
    assert_is_utc_datetime(datetime_start)
    assert_is_utc_datetime(datetime_end)
    ras(isinstance(step_timedelta, datetime.timedelta))
    ras(datetime_start < datetime_end)
    ras(step_timedelta > datetime.timedelta(0))

    crrt_time = datetime_start
    yield crrt_time

    while True:
        crrt_time += step_timedelta
        if crrt_time < datetime_end:
            yield crrt_time
        else:
            break
