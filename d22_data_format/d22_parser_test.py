import datetime
import pytz

import pprint

import logging

from d22_data_format.d22_parser import D22Parser
from d22_data_format.helpers.load_test_data import path_to_test_data, get_parsed_correct_data


def test_parse_short():
    path_to_d22_file = path_to_test_data("short_20130923.d22")
    dict_correct_parsed_data = get_parsed_correct_data("short_20130923.py")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing() 

    assert dict_correct_parsed_data == dict_result


def test_selection_parse_long():
    path_to_d22_file = path_to_test_data("20130923.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing() 

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 0, tzinfo=pytz.utc)]\
        ["MSB"]["list_entries"] == [4.5, 23.51, 3.73, 0.01]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 40, tzinfo=pytz.utc)]\
        ["MD1"]["list_entries"] == [2.0]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 18, 0, tzinfo=pytz.utc)]\
        ["TH12"]["list_entries"] == [-999.99, -999.99, 78.59]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 23, 50, tzinfo=pytz.utc)]\
        ["WL1"]["list_entries"] == [56.97, -56.97, 56.05, 57.86, -57.86, -56.05]


def test_selection_parse_long_repeat():
    path_to_d22_file = path_to_test_data("20130923_repeat.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing() 

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 0, tzinfo=pytz.utc)]\
        ["MSB"]["list_entries"] == [4.5, 23.51, 3.73, 0.01]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 0, 40, tzinfo=pytz.utc)]\
        ["MD1"]["list_entries"] == [2.0]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 18, 0, tzinfo=pytz.utc)]\
        ["TH12"]["list_entries"] == [-999.99, -999.99, 78.59]

    assert dict_result["Heimdal"][datetime.datetime(2013, 9, 23, 23, 50, tzinfo=pytz.utc)]\
        ["WL1"]["list_entries"] == [56.97, -56.97, 56.05, 57.86, -57.86, -56.05]


def test_parse_gz_check_selection():
    path_to_d22_file = path_to_test_data("20020927.d22.gz")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    assert dict_result["Heimdal"][datetime.datetime(2002, 9, 27, 0, 0, tzinfo=pytz.utc)]\
        ["MSA"]["list_entries"] == [5.93, 22.21, 5.12, 23.18]

    assert dict_result["Heimdal"][datetime.datetime(2002, 9, 27, 0, 0, tzinfo=pytz.utc)]\
        ["MTB"]["list_entries"] == [6.07, 22.25, 5.39, 22.29]

    assert dict_result["Heimdal"][datetime.datetime(2002, 9, 27, 13, 0, tzinfo=pytz.utc)]\
        ["WL1"]["list_entries"] == [57.00, 0.50, 56.02, 57.91, -999.99, -999.99]


def test_parser_problematic_file_1():
    path_to_d22_file = path_to_test_data("20150101.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    assert dict_result["1021"][datetime.datetime(2015, 1, 1, 23, 0, tzinfo=pytz.utc)]\
        ["MD1"]["list_entries"] == [359.50]

    assert dict_result["1021"][datetime.datetime(2015, 1, 1, 23, 50, tzinfo=pytz.utc)]\
        ["MD1"]["list_entries"] == [359.50]

    assert dict_result["1021"][datetime.datetime(2015, 1, 1, 23, 50, tzinfo=pytz.utc)]\
        ["VG1"]["list_entries"] == [-999.99, 270.00]

    assert dict_result["1021"][datetime.datetime(2015, 1, 1, 18, 10, tzinfo=pytz.utc)]\
        ["MD1"]["list_entries"] == [359.50]


def test_parser_problematic_file_2():
    path_to_d22_file = path_to_test_data("20130713.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    assert dict_result["DRAUGEN"][datetime.datetime(2013, 7, 13, 0, 1, tzinfo=pytz.utc)]\
        ["ST1"]["list_entries"] == [10.18]

    assert dict_result["DRAUGEN"][datetime.datetime(2013, 7, 13, 0, 1, tzinfo=pytz.utc)]\
        ["MTB"]["list_entries"] == [9.16, 23.48, 8.09, 23.57]

    assert dict_result["DRAUGEN"][datetime.datetime(2013, 7, 13, 21, 31, tzinfo=pytz.utc)]\
        ["WL1"]["list_entries"] == [30.81, 2.19, 29.36, 32.13, 0.88, 3.64]

    assert dict_result["DRAUGEN"][datetime.datetime(2013, 7, 13, 0, 1, tzinfo=pytz.utc)]\
        ["ST1"]["list_entries"] == [10.18]


def test_parser_problematic_file_3():
    path_to_d22_file = path_to_test_data("20010322.d22.gz")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    assert dict_result["Ekofisk"][datetime.datetime(2001, 3, 22, 0, 0, tzinfo=pytz.utc)]\
        ["MSA"]["list_entries"] == [12.97, 23.54, 9.55, 23.57]

    assert dict_result["Ekofisk"][datetime.datetime(2001, 3, 22, 22, 0, tzinfo=pytz.utc)]\
        ["MTA"]["list_entries"] == [11.65, 12.47, 8.61, 12.04]


def test_parser_draugen_2001():
    path_to_d22_file = path_to_test_data("20010831.d22.gz")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

def test_parser_DF15():
    path_to_d22_file = path_to_test_data("19970307.d22.gz")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()


# TODO: should add tests on the file 20200419.d22 , around line 31233 where missing
# / faulty transmission, to check that parsing around is fine

if __name__ == "__main__":
    # TODO: logging info, add run of parsing of problematic file
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.INFO)

    path_to_d22_file = path_to_test_data("20010831.d22.gz")

    print("START TO PARSE")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    pp = pprint.PrettyPrinter(indent=4).pprint
    pp(dict_result)
