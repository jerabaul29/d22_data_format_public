# create a summary of available data blocks across platforms, times, etc

# Go through all platforms
# Go through all data files
# Grab only the data block titles with timestamps

# allow to ask questions:
# show all data blocks one platform
# list all platforms that have data blocks with header types

# there are different kinds of ways data are stored: year, year / month, year / month / day
# all that have d22 data seem to have it "at the root" of the platform folder
import logging

import os

from pathlib import Path

import datetime
import pytz

import pickle

import pprint

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from d22_data_format.d22_parser import D22Parser
from d22_data_format.exploration_tools import d22_global_dirs_generator
from d22_data_format.helpers.load_test_data import path_to_test_data
from d22_data_format.name_lookups import list_stations_ids, dict_ids_lookup

def extend_dict_metadata(dict_metadata, d22_file):
    try:
        dict_parsed_data = D22Parser(path_to_d22_file=d22_file).perform_parsing()

        station_name = list(dict_parsed_data.keys())[0]
        if station_name not in dict_metadata:
            logging.warning("Add new station: {}".format(station_name))
            dict_metadata[station_name] = {}

        for crrt_time in dict_parsed_data[station_name]:
            for crrt_header in dict_parsed_data[station_name][crrt_time]:
                if crrt_header not in dict_metadata[station_name]:
                    logging.warning("Add new header: {}".format(crrt_header))
                    dict_metadata[station_name][crrt_header] = {}
                    dict_metadata[station_name][crrt_header]["all_timestamps"] = []
                dict_metadata[station_name][crrt_header]["all_timestamps"].append(crrt_time)

    except Exception as crrt_except:
        logging.error("attempting to parse file: {}".format(d22_file))
        logging.error("but received an error: {}".format(crrt_except))
        logging.error("dropping to log the current file!")

    return dict_metadata


def generate_datablocks_overview_dict(path_root_data=None, list_stations_subfolders=None):
    dict_metadata = {}

    for crrt_folder in d22_global_dirs_generator(path_root_data=path_root_data,
                                                 list_stations_subfolders=list_stations_subfolders):
        files = sorted([x for x in Path(crrt_folder).glob("*") if x.is_file()])
        logging.info("found files: {}".format(files))

        for crrt_file in files:
            crrt_file = str(crrt_file)
            if not (crrt_file[-4:] == ".d22" or crrt_file[-7:] == ".d22.gz"):
                logging.warning("found a non d22 file: {}".format(crrt_file))
                continue

            logging.info("get header information for {}".format(crrt_file))
            dict_metadata = extend_dict_metadata(dict_metadata, crrt_file)

    return dict_metadata

def order_dict_metadata(dict_metadata):
    for crrt_station in dict_metadata:
        crrt_station_dict = dict_metadata[crrt_station]
        for crrt_header in crrt_station_dict:
            dict_metadata[crrt_station][crrt_header]["all_timestamps"].sort()


def generate_stats_on_dict_metadata(dict_metadata, print_info=False):
    order_dict_metadata(dict_metadata)

    dict_pure_metadata = {}

    max_acceptable_data_timedelta = datetime.timedelta(hours=2.0)

    for crrt_station in dict_metadata:
        crrt_station_dict = dict_metadata[crrt_station]
        dict_pure_metadata[crrt_station] = {}
        for crrt_header in crrt_station_dict:
            dict_pure_metadata[crrt_station][crrt_header] = {}
            crrt_station_header_dict = crrt_station_dict[crrt_header]
            crrt_station_metadata_dict = dict_pure_metadata[crrt_station][crrt_header]

            crrt_station_header_timestamps = crrt_station_header_dict["all_timestamps"]
            crrt_station_header_dict["first_timestamp"] = crrt_station_header_timestamps[0]
            crrt_station_header_dict["last_timestamp"] = crrt_station_header_timestamps[-1]
            crrt_station_header_dict["data_dropouts"] = []

            for crrt_timestamp, next_timestamp in zip(crrt_station_header_timestamps[:-1],
                                                      crrt_station_header_timestamps[1:]):
                if (next_timestamp - crrt_timestamp) > max_acceptable_data_timedelta:
                    logging.warning("found a timedelta {} looking at station {}, header {}, timestamp {} to {}".format(
                        next_timestamp - crrt_timestamp,
                        crrt_station,
                        crrt_header,
                        crrt_timestamp,
                        next_timestamp
                    )) 
                    crrt_station_header_dict["data_dropouts"].append((crrt_timestamp, next_timestamp))

            crrt_station_metadata_dict["first_timestamp"] = crrt_station_header_dict["first_timestamp"]
            crrt_station_metadata_dict["last_timestamp"] = crrt_station_header_dict["last_timestamp"]
            crrt_station_metadata_dict["data_dropouts"] = crrt_station_header_dict["data_dropouts"]

            if print_info:
                print("station {}, header {} has data from {} to {}".format(
                    crrt_station,
                    crrt_header,
                    crrt_station_header_timestamps[0],
                    crrt_station_header_timestamps[-1]
                ))
                if crrt_station_header_dict["data_dropouts"]:
                    print("the following dropouts were found:")
                    for crrt_dropout in crrt_station_header_dict["data_dropouts"]:
                        print("{} to {}".format(crrt_dropout[0], crrt_dropout[1]))

    return dict_pure_metadata


def show_summary_blocks_one_station(dict_pure_metadata, station_name):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(18, 5))

    if station_name not in dict_pure_metadata:
        logging.error("station name should be a key of dict_pure_metadata; these are:")
        logging.error(list(dict_pure_metadata.keys()))
        logging.error("but got for station_name: {}".format(station_name))

        raise ValueError("unknown station name")

    global_crrt_max_time = datetime.datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
    global_crrt_min_time = datetime.datetime(4000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

    list_crrt_block_index = []
    list_crrt_block = []
    list_crrt_min_time = []
    list_crrt_max_time = []
    list_crrt_color = []

    for crrt_block_index, crrt_block in enumerate(sorted(dict_pure_metadata[station_name])):
        if crrt_block_index % 2 == 0:
            crrt_color = "b"
        else:
            crrt_color = 'k'

        crrt_min_time = dict_pure_metadata[station_name][crrt_block]["first_timestamp"]
        crrt_max_time = dict_pure_metadata[station_name][crrt_block]["last_timestamp"]

        if crrt_max_time > global_crrt_max_time:
            global_crrt_max_time = crrt_max_time

        if crrt_min_time < global_crrt_min_time:
            global_crrt_min_time = crrt_min_time

        plt.plot([crrt_min_time, crrt_max_time], [crrt_block_index, crrt_block_index],
                 linewidth=3.5, color=crrt_color)

        for crrt_dropout in dict_pure_metadata[station_name][crrt_block]["data_dropouts"]:
            plt.plot([crrt_dropout[0], crrt_dropout[1]], [crrt_block_index, crrt_block_index],
                     linewidth=3.5, color='r')

        list_crrt_block_index.append(crrt_block_index)
        list_crrt_block.append(crrt_block)
        list_crrt_min_time.append(crrt_min_time)
        list_crrt_max_time.append(crrt_max_time)
        list_crrt_color.append(crrt_color)

    location_start = global_crrt_max_time + 0.01 * (global_crrt_max_time - global_crrt_min_time)

    for (crrt_block_index,
         crrt_block,
         crrt_min_time,
         crrt_max_time,
         crrt_color) \
        in zip(list_crrt_block_index,
               list_crrt_block,
               list_crrt_min_time,
               list_crrt_max_time,
               list_crrt_color):

        plt.text(location_start, crrt_block_index-0.5,
                 "#{:02}{} {}.{:02}-{}.{:02}".format(crrt_block_index,
                                                     crrt_block,
                                                     crrt_min_time.year,
                                                     crrt_min_time.month,
                                                     crrt_max_time.year,
                                                     crrt_max_time.month),
                 color=crrt_color)

    mpl_min_time = mdates.date2num(global_crrt_min_time)
    mpl_max_time = mdates.date2num(global_crrt_max_time + 0.18 * (global_crrt_max_time - global_crrt_min_time))

    plt.xlim([mpl_min_time, mpl_max_time])
    plt.ylabel("blocks station {}".format(station_name))

    plt.show() 


def show_summary_blocks_across_stations(dict_pure_metadata, list_block_prefixes):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(18, 5))

    global_crrt_max_time = datetime.datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
    global_crrt_min_time = datetime.datetime(4000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

    crrt_used_station_ind = 0
    crrt_plotting_ind = -1

    list_crrt_plotting_ind = []
    list_crrt_station = []
    list_crrt_station_name = []
    list_crrt_block = []
    list_crrt_min_time = []
    list_crrt_max_time = []
    list_crrt_color = []

    for crrt_station in dict_pure_metadata:
        crrt_list_blocks = list(dict_pure_metadata[crrt_station].keys())

        list_selected_bocks = []

        for crrt_block in crrt_list_blocks:
            for crrt_prefix in list_block_prefixes:
                if crrt_prefix in crrt_block:
                    list_selected_bocks.append(crrt_block)

        if list_selected_bocks:
            crrt_used_station_ind += 1

        if crrt_used_station_ind % 2 == 0:
            crrt_color = "b"
        else:
            crrt_color = 'k'

        for crrt_block in list_selected_bocks:
            crrt_plotting_ind += 1

            crrt_min_time = dict_pure_metadata[crrt_station][crrt_block]["first_timestamp"]
            crrt_max_time = dict_pure_metadata[crrt_station][crrt_block]["last_timestamp"]

            if crrt_max_time > global_crrt_max_time:
                global_crrt_max_time = crrt_max_time

            if crrt_min_time < global_crrt_min_time:
                global_crrt_min_time = crrt_min_time

            plt.plot([crrt_min_time, crrt_max_time], [crrt_plotting_ind, crrt_plotting_ind],
                     linewidth=3.5, color=crrt_color)

            list_crrt_plotting_ind.append(crrt_plotting_ind)
            list_crrt_station.append(crrt_station)
            list_crrt_station_name.append(dict_ids_lookup[crrt_station])
            list_crrt_block.append(crrt_block)
            list_crrt_min_time.append(crrt_min_time)
            list_crrt_max_time.append(crrt_max_time)
            list_crrt_color.append(crrt_color)

            for crrt_dropout in dict_pure_metadata[crrt_station][crrt_block]["data_dropouts"]:
                plt.plot([crrt_dropout[0], crrt_dropout[1]], [crrt_plotting_ind, crrt_plotting_ind],
                         linewidth=3.5, color='r')

    if crrt_plotting_ind == -1:
        raise ValueError("no data to plot! Are you sure about the prefixes?")

    crrt_plotting_ind = -1
    crrt_used_station_ind = 0

    location_start = global_crrt_max_time + 0.01 * (global_crrt_max_time - global_crrt_min_time)

    for (crrt_plotting_ind,
         crrt_station_name,
         crrt_station,
         crrt_block,
         crrt_min_time,
         crrt_max_time,
         crrt_color) \
        in zip(list_crrt_plotting_ind,
               list_crrt_station_name,
               list_crrt_station,
               list_crrt_block,
               list_crrt_min_time,
               list_crrt_max_time,
               list_crrt_color):

        plt.text(location_start, crrt_plotting_ind,
                 "#{:02}: {}/{} {} {}.{:02}-{}.{:02}".format(crrt_plotting_ind,
                                                             crrt_station_name,
                                                             crrt_station,
                                                             crrt_block,
                                                             crrt_min_time.year,
                                                             crrt_min_time.month,
                                                             crrt_max_time.year,
                                                             crrt_max_time.month),
                 color=crrt_color)

    mpl_min_time = mdates.date2num(global_crrt_min_time)
    mpl_max_time = mdates.date2num(global_crrt_max_time + 0.31 * (global_crrt_max_time - global_crrt_min_time))

    plt.xlim([mpl_min_time, mpl_max_time])
    plt.ylabel("blocks")

    plt.show() 

# TODO: when finding dropouts: set to dropout if for example 50% or more of the data are error messages.

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    pp = pprint.PrettyPrinter(indent=2).pprint

    if False:
        dict_metadata = generate_datablocks_overview_dict(path_root_data=path_to_test_data() + "/sample/",
                                                          )
        dict_pure_metadata = generate_stats_on_dict_metadata(dict_metadata, print_info=True)

        pp(dict_pure_metadata)

        if Path("dict_metadata.pkl").is_file():
            os.remove("dict_metadata.pkl")

        if Path("dict_pure_metadata.pkl").is_file():
            os.remove("dict_pure_metadata.pkl")

        with open('dict_metadata.pkl', 'wb') as fh:
            pickle.dump(dict_metadata, fh, protocol=pickle.HIGHEST_PROTOCOL)

        with open('dict_pure_metadata.pkl', 'wb') as fh:
            pickle.dump(dict_pure_metadata, fh, protocol=pickle.HIGHEST_PROTOCOL)

    with open('dict_pure_metadata.pkl', 'rb') as fh:
        dict_pure_metadata = pickle.load(fh)

    show_summary_blocks_one_station(dict_pure_metadata, "Gullfaks C")
    show_summary_blocks_one_station(dict_pure_metadata, "WNG")
    show_summary_blocks_one_station(dict_pure_metadata, "AASTA")
    # show_summary_blocks_one_station(dict_pure_metadata, "West Navigator")

    show_summary_blocks_across_stations(dict_pure_metadata, list_block_prefixes=["CL"])
    show_summary_blocks_across_stations(dict_pure_metadata, list_block_prefixes=["WL"])

    list_keys_stations = list(dict_pure_metadata.keys())
    pp(list_keys_stations)

    for crrt_station in list_keys_stations:
        if crrt_station not in list_stations_ids:
            logging.warning("station {} was found in metadata but not in name lookup!".format(crrt_station))
