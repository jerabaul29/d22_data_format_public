import datetime
import pytz

import math

from d22_data_format.data_extractor import DataExtractor, DataSpec
from d22_data_format.helpers.load_test_data import path_to_test_data

def test_1():
    list_data_specs = [DataSpec("aastahansteen", "AASTA", "MD1", "magnetic_declination"),
                       DataSpec("aastahansteen", "AASTA", "MTB", "max_gust_last_period")]

    data_extractor = DataExtractor(path_root_data=path_to_test_data() + "/sample_AASTA/")
    dict_result = data_extractor.extract_available_data(list_data_specs)

    assert dict_result[list_data_specs[0]][0][1] == 359.0

    datetime_start = datetime.datetime(2019, 9, 17, 0, 0, 0, tzinfo=pytz.utc)
    datetime_end = datetime.datetime(2019, 9, 30, 0, 0, 0, tzinfo=pytz.utc)
    datetime_resolution = datetime.timedelta(minutes=10)

    dict_time_series = data_extractor.data_as_time_series(datetime_start, datetime_end, datetime_resolution)

    print(dict_time_series)

    assert math.isnan(data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 17, 23, 0, 0, tzinfo=pytz.utc),
        list_data_specs[0])
    )

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 18, 0, 0, 0, tzinfo=pytz.utc),
        list_data_specs[0]) == 359.00

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 21, 1, 20, 0, tzinfo=pytz.utc),
        list_data_specs[0]) == 359.00

    assert math.isnan(data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 21, 1, 30, 0, tzinfo=pytz.utc),
        list_data_specs[0])
    )

    assert math.isnan(data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 21, 2, 30, 0, tzinfo=pytz.utc),
        list_data_specs[0])
    )

    assert math.isnan(data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 21, 6, 0, 0, tzinfo=pytz.utc),
        list_data_specs[0])
    )

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 21, 6, 10, 0, tzinfo=pytz.utc),
        list_data_specs[0]) == 359.00

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 18, 0, 0, 0, tzinfo=pytz.utc),
        list_data_specs[1]) == 10.02

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 18, 0, 10, 0, tzinfo=pytz.utc),
        list_data_specs[1]) == 10.02

    assert data_extractor.extract_value_from_time_series(
        datetime.datetime(2019, 9, 18, 18, 50, 0, tzinfo=pytz.utc),
        list_data_specs[1]) == 11.81

    data_extractor.plot_time_series(
        list_specs=[DataSpec("aastahansteen", "AASTA", "MTB", "max_gust_last_period")])

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARN)
    test_1()
