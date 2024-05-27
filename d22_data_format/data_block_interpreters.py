"""Implement individual data block interpreters."""

import math
import logging

import datetime

import pprint

from d22_data_format.helpers.raise_assert import ras
from d22_data_format.d22_parser import D22Parser
from d22_data_format.helpers.load_test_data import path_to_test_data

dict_error_codes = {
    -999.99: "not_measured",
    -999.88: "failed_quality",
    -999.77: "out_of_range" ,
}

def perform_substitution_errors(dict_in):
    for crrt_key in dict_in:
        crrt_entry = dict_in[crrt_key]
        if crrt_entry in dict_error_codes:
            dict_in[crrt_key] = math.nan


class MissingBlockProcessor():
    def __init__(self):
        self.list_unknown_blocks = []

    def register_missing_block_processor(self, full_block_title):
        block_type = get_block_type(full_block_title)

        if block_type not in self.list_unknown_blocks:
            self.list_unknown_blocks.append(block_type)
            logging.info("block type {} has no registered processor".format(block_type))


def get_block_type(full_block_title):
    return(full_block_title[0:2])

############################################################
########## put the data bloc analyzers under
############################################################

def raw_WL_to_extracted_dict(dict_raw_WL_block):
    dict_result = {}

    try:
        list_entries = dict_raw_WL_block["list_entries"]

        if len(list_entries) == 6:
            ras(isinstance(dict_raw_WL_block, dict))

            ras(len(list_entries) == 6)

            dict_result["average_air_gap"] = list_entries[0]
            dict_result["average_water_level_ref_LAT"] = list_entries[1]
            dict_result["minimum_air_gap"] = list_entries[2]
            dict_result["maximum_air_gap"] = list_entries[3]
            dict_result["min_water_level_ref_LAT"] = list_entries[4]
            dict_result["max_water_level_ref_LAT"] = list_entries[5]

            perform_substitution_errors(dict_result)

        elif len(list_entries) == 4:
            ras(isinstance(dict_raw_WL_block, dict))

            ras(len(list_entries) == 4)

            dict_result["average_air_gap"] = list_entries[0]
            dict_result["average_water_level_ref_LAT"] = list_entries[1]
            dict_result["minimum_air_gap"] = list_entries[2]
            dict_result["maximum_air_gap"] = list_entries[3]
            dict_result["min_water_level_ref_LAT"] = math.nan
            dict_result["max_water_level_ref_LAT"] = math.nan

            perform_substitution_errors(dict_result)

    except:
        logging.error("problem processing WL block, filling with NaN. Block:")
        logging.error(dict_raw_WL_block)

    return dict_result

def raw_MD_to_extracted_dict(dict_raw_MD_block):
    dict_result = {}

    try:
        ras(isinstance(dict_raw_MD_block, dict))

        list_entries = dict_raw_MD_block["list_entries"]
        ras(len(list_entries) == 1)

        dict_result["magnetic_declination"] = list_entries[0]

        perform_substitution_errors(dict_result)
    except:
        logging.error("problem processing MD block, filling with NaN. Block:")
        logging.error(dict_raw_MD_block)

        dict_result["magnetic_declination"] = math.nan

    return dict_result

def raw_MT_to_extracted_dict(dict_raw_MT_block):
    def datetime_minute_hour_from_decimal(decimal_in):
        hour = int(decimal_in)
        minute = int((decimal_in - int(decimal_in)) * 10)
        error = False

        if not 0 <= hour and hour <= 23:
            logging.info("got invalid hour from input {}: {}".format(decimal_in, hour))
            error = True

        if not 0 <= minute and minute <= 59:
            logging.info("got invalid minute from input {}: {}".format(decimal_in, minute))
            error = True

        if error:
            return math.nan
        else:
            result = datetime.datetime(year=1, month=1, day=1,
                                       hour=hour, minute=minute)

        return result

    dict_result = {}

    try:
        ras(isinstance(dict_raw_MT_block, dict))

        list_entries = dict_raw_MT_block["list_entries"]
        ras(len(list_entries) == 4)

        dict_result["max_gust_last_period"] = list_entries[0]
        dict_result["time_max_gust"] = datetime_minute_hour_from_decimal(list_entries[1])
        dict_result["max_average_wind_speed_last_period"] = list_entries[2]
        dict_result["time_max_average"] = datetime_minute_hour_from_decimal(list_entries[3])

        perform_substitution_errors(dict_result)
    except:
        logging.error("problem processing MT block, filling with NaN. Block:")
        logging.error(dict_raw_MT_block)

        dict_result["max_gust_last_period"] = math.nan
        dict_result["time_max_gust"] = math.nan
        dict_result["max_average_wind_speed_last_period"] = math.nan
        dict_result["time_max_average"] = math.nan

    return dict_result

############################################################
########## fill in the block analyzers dict under
############################################################

dict_block_processers = {
    "WL": raw_WL_to_extracted_dict,
    "MD": raw_MD_to_extracted_dict,
    "MT": raw_MT_to_extracted_dict,
}

############################################################
########## the main function to go through the parsed dict
############################################################

def process_dict_blocks(dict_in):
    ras(isinstance(dict_in, dict)) 

    missing_block_processor = MissingBlockProcessor()

    for crrt_platform in dict_in :
        crrt_platform_dict = dict_in[crrt_platform]
        for crrt_datetime in crrt_platform_dict:
            crrt_package_dict = crrt_platform_dict[crrt_datetime]
            for crrt_block_fulltitle in list(crrt_package_dict.keys()):
                crrt_block_type = get_block_type(crrt_block_fulltitle)

                if crrt_block_type not in dict_block_processers:
                    missing_block_processor.register_missing_block_processor(crrt_block_type)

                else:
                    crrt_processer = dict_block_processers[crrt_block_type]
                    crrt_extracted_dict = crrt_processer(crrt_package_dict[crrt_block_fulltitle])
                    crrt_package_dict[crrt_block_fulltitle]["extracted"] = crrt_extracted_dict


if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=1).pprint

    path_to_d22_file = path_to_test_data("short_20130923.d22")

    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()
    pp(dict_result)

    process_dict_blocks(dict_result)
    pp(dict_result)

# TODO: add test
# TODO: create class to build nc4 dataset of a given type
# TODO: in nc4 dataset, put the geographic positions
# NOTE: cannot be water level relative to LAT in meters, this is far too big values!
