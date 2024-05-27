import datetime
import pytz

import math

from d22_data_format.d22_parser import D22Parser
from d22_data_format.data_block_interpreters import process_dict_blocks
from d22_data_format.helpers.load_test_data import path_to_test_data

def test_1():
    path_to_d22_file = path_to_test_data("short_20130923.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()
    process_dict_blocks(dict_result)

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 0, tzinfo=pytz.utc)]["WL1"]\
        ["extracted"]["average_air_gap"] == 56.72

    assert math.isnan(dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 0, tzinfo=pytz.utc)]["WL1"]
                      ["extracted"]["max_water_level_ref_LAT"])
