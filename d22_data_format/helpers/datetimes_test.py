import datetime
import pytz

from d22_data_format.helpers.datetimes import datetime_range

def test_datetime_range_1():
    datetime_start = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
    datetime_end = datetime.datetime(2020, 1, 3, 0, 0, 0, tzinfo=pytz.utc)
    step_timedelta = datetime.timedelta(days=1)

    result = datetime_range(datetime_start, datetime_end, step_timedelta)

    correct_result = [datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
                      datetime.datetime(2020, 1, 2, 0, 0, 0, tzinfo=pytz.utc),
                      ]

    assert list(result) == correct_result


def test_datetime_range_2():
    datetime_start = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
    datetime_end = datetime.datetime(2020, 1, 5, 0, 0, 0, tzinfo=pytz.utc)
    step_timedelta = datetime.timedelta(days=2)

    result = datetime_range(datetime_start, datetime_end, step_timedelta)

    correct_result = [datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
                      datetime.datetime(2020, 1, 3, 0, 0, 0, tzinfo=pytz.utc),
                      ]

    assert list(result) == correct_result
