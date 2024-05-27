# extract data following some requests: which stations, which blocks, which fields
# TODO: conversion station folder name / station ID in d22
# TODO: function to extract from 1 parsed dir
# TODO: function to loop on all relevant d22 files

# TODO: function to interpolate on common time base

# 1) extraction: folder / blocks: extract all available data
# 2) putting on a dataset: intepolate to time base, check quality, fill holes, dump as nc4

# quality checks
# tools for "fusioning" the data

import logging
import math

import matplotlib.pyplot as plt

import datetime

from d22_data_format.helpers.datetimes import assert_is_utc_datetime, datetime_range

from d22_data_format.d22_parser import D22Parser
from d22_data_format.data_block_interpreters import process_dict_blocks
from d22_data_format.exploration_tools import d22_files_in_dir_generator
from d22_data_format.helpers.raise_assert import ras


class DataSpec():
    def __init__(self, folder, station_id, block_id, block_field):
        self.folder = folder
        self.station_id = station_id
        self.block_id = block_id
        self.block_field = block_field

    def __eq__(self, other): 
        if not isinstance(other, DataSpec):
            return ValueError("can only compare equality with other specs")

        return (
            self.folder == other.folder and   
            self.station_id == other.station_id and
            self.block_id == other.block_id and
            self.block_field == other.block_field
        )

    def __hash__(self):
        return hash((self.folder, self.station_id, self.block_id, self.block_field))

    def __str__(self):
        return "folder {} / id {} / block {} / {}".format(self.folder,
                                                          self.station_id,
                                                          self.block_id,
                                                          self.block_field)


class DataExtractor():
    def __init__(self, path_root_data=None):
        if path_root_data is None:
            path_root_data = "/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/"

        self.path_root_data = path_root_data
        self.data_files_yielder = None

        self.extract_available_data_is_run = False
        self.data_as_time_series_is_run = False

    def set_data_files_yielder(self, folder):
        folder_as_list = [folder]
        self.data_files_yielder = d22_files_in_dir_generator(path_root_data=self.path_root_data,
                                                             list_stations_subfolders=folder_as_list)

    def extract_available_data(self, list_data_specs_same_folder):
        # list_data_specs: list of data_specs, each with 1) folder, 2) station_id, 3) block_id, 4) block_field

        # 1: check that all from same folder
        # 2: generate the files yielder
        # 3: file by file, parse, and extract the data of interest. Put it in a dict

        self.list_data_specs_same_folder = list_data_specs_same_folder

        # 1.
        for crrt_spec in list_data_specs_same_folder:
            ras(crrt_spec.folder == list_data_specs_same_folder[0].folder)

        # 2.
        crrt_folder = list_data_specs_same_folder[0].folder
        self.set_data_files_yielder(crrt_folder)

        # 3.
        self.dict_gathered_data = {}
        for crrt_spec in list_data_specs_same_folder:
            self.dict_gathered_data[crrt_spec] = []

        for crrt_file in self.data_files_yielder:
            logging.info("looking at {}".format(crrt_file)) 

            try:

                # extract information and label fields
                d22_parser = D22Parser(path_to_d22_file=crrt_file)
                crrt_dict_result = d22_parser.perform_parsing()
                process_dict_blocks(crrt_dict_result)

                # go through all specs, and append the results at the right location
                for crrt_station_id in crrt_dict_result:
                    for crrt_timestamp in crrt_dict_result[crrt_station_id]:
                        for crrt_block_id in crrt_dict_result[crrt_station_id][crrt_timestamp]:
                            if "extracted" in crrt_dict_result[crrt_station_id][crrt_timestamp][crrt_block_id]:
                                for crrt_block_field in crrt_dict_result[crrt_station_id][crrt_timestamp]\
                                        [crrt_block_id]["extracted"]:
                                    matching_spec = self.find_spec(crrt_station_id,
                                                                   crrt_block_id, crrt_block_field)

                                    if matching_spec is not None:
                                        crrt_value = crrt_dict_result[crrt_station_id][crrt_timestamp]\
                                            [crrt_block_id]["extracted"][crrt_block_field]
                                        self.dict_gathered_data[matching_spec].append((crrt_timestamp, crrt_value))

                self.extract_available_data_is_run = True

            except:
                pass

        return self.dict_gathered_data

    def find_spec(self, crrt_station_id, crrt_block_id, crrt_block_field):
        for crrt_spec in self.list_data_specs_same_folder:
            if (
                crrt_spec.station_id == crrt_station_id and
                crrt_spec.block_id == crrt_block_id and
                crrt_spec.block_field == crrt_block_field
            ):
                return crrt_spec

        return None

    def data_as_time_series(self, datetime_start, datetime_end, datetime_resolution):
        # use the dict of data generated to provide time series, in a dict
        # 1. double check that the specs are ordered
        # 2. put the data, interpolate / fill with NaNs where necessary

        ras(self.extract_available_data_is_run, "need to call extract_available_data first!")

        assert_is_utc_datetime(datetime_start)
        assert_is_utc_datetime(datetime_end)
        ras(isinstance(datetime_resolution, datetime.timedelta))

        self.dict_time_series = {}
        self.dict_time_series["timestamps"] = list(datetime_range(datetime_start, datetime_end, datetime_resolution))

        for crrt_spec in self.dict_gathered_data:
            self.dict_gathered_data[crrt_spec].sort(key=lambda x: x[0])
            for crrt_entry, next_entry in zip(self.dict_gathered_data[crrt_spec][:-1],
                                              self.dict_gathered_data[crrt_spec][1:]):
                try:
                    ras(crrt_entry[0] <= next_entry[0])
                except:
                    logging.error("got badly ordered entries in {}: {} and {} are not ordered"
                                  .format(
                                      crrt_spec,
                                      crrt_entry[0],
                                      next_entry[0]
                                  )
                                  )

        datetime_tolerance = datetime_resolution / 2

        for crrt_spec in self.dict_gathered_data:
            crrt_entry_index = 0
            max_entry_index = len(self.dict_gathered_data[crrt_spec]) - 1
            self.dict_time_series[crrt_spec] = []

            for crrt_timestamp in self.dict_time_series["timestamps"]:

                while (
                    self.dict_gathered_data[crrt_spec][crrt_entry_index][0] < crrt_timestamp - datetime_tolerance and
                    crrt_entry_index < max_entry_index
                ):
                    crrt_entry_index += 1

                if (
                    crrt_entry_index > max_entry_index or
                    abs(crrt_timestamp - self.dict_gathered_data[crrt_spec][crrt_entry_index][0]) > datetime_tolerance
                ):
                    self.dict_time_series[crrt_spec].append(math.nan)
                else:
                    self.dict_time_series[crrt_spec].append(self.dict_gathered_data[crrt_spec][crrt_entry_index][1])
                    if crrt_entry_index < max_entry_index:
                        crrt_entry_index += 1

            ras(len(self.dict_time_series["timestamps"]) == len(self.dict_time_series[crrt_spec]))

        self.data_as_time_series_is_run = True

        return self.dict_time_series

    def extract_value_from_time_series(self, datetime_of_sample, spec):
        ras(self.data_as_time_series_is_run, "need to call data_as_time_series first!")

        assert_is_utc_datetime(datetime_of_sample)
        ras(isinstance(spec, DataSpec))
        ras(datetime_of_sample in self.dict_time_series["timestamps"])
        ras(spec in self.dict_time_series)

        index_of_datetime = self.dict_time_series["timestamps"].index(datetime_of_sample)

        return self.dict_time_series[spec][index_of_datetime]

    def plot_time_series(self, list_specs=None):
        if list_specs is None:
            list_specs = [x for x in self.dict_time_series if isinstance(x, DataSpec)]

        ras(self.data_as_time_series_is_run, "need to call data_as_time_series first!")

        for crrt_spec in list_specs:
            ras(crrt_spec in self.list_data_specs_same_folder)

        plt.figure()

        for crrt_spec in list_specs:
            plt.plot(self.dict_time_series["timestamps"],
                     self.dict_time_series[crrt_spec],
                     label="{}: {}/{}".format(crrt_spec.station_id,
                                              crrt_spec.block_id,
                                              crrt_spec.block_field))

        plt.legend()
        plt.show()
