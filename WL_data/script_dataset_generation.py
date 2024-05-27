"""generate a netCDF4 dataset with some specific variables over a given time interval"""

import logging

import pickle

import datetime
import pytz

import tqdm

import matplotlib.pyplot as plt

import numpy as np

import netCDF4 as nc4

from d22_data_format.datablocs_summary import show_summary_blocks_across_stations
from d22_data_format.data_extractor import DataSpec, DataExtractor
from d22_data_format.helpers.datetimes import datetime_range, assert_is_utc_datetime
from d22_data_format.helpers.raise_assert import ras
from d22_data_format.helpers.filters import three_stages_hampel, interpolate_short_dropouts, hampel, \
    butter_bandpass_filter
from d22_data_format.helpers.filters import outlier_dropout_processing, running_average
from d22_data_format.name_lookups import dict_name_to_position

logging.basicConfig(level=logging.INFO)

# here, we will generate a dataset of WL (water level) data
# I use WL as this is the data I needed for my project; of course, other blocks
# may be processed similarly.

# there is anyways quite a bit of work that needs to be done "by hand",
# so we keep it as a script with comments, though some parts could be
# wrapped in classes later on if needed.

# I switch on and off blocks of code with if True / False. A bit ugly, but but...

############################################################
############################################################
############### finding which stations to use
############################################################
############################################################

# the first step is to have a look at which WL data are available form which stations
with open('../d22_data_format/dict_pure_metadata.pkl', "rb") as fh:
    dict_pure_metadata = pickle.load(fh)

if False:
    show_summary_blocks_across_stations(dict_pure_metadata, list_block_prefixes=["WL"])

# based on this, we can define a series of specs in different station folders
# for which we want to extract data.
# out of this, we generate a pkl dataset
# we take data from the stations that have "a lot" of data
path_WL_data = "./dict_data_WL.pkl"
path_interpolated_outlier_removed_data = "./dict_data_WL_interpolated_outliers_removed.pkl"

# heimdal has some data
list_specs_heimdal = [DataSpec("/heimdal/", "Heimdal", "WL1", "average_water_level_ref_LAT")]

# ekofisk has some data
list_specs_ekofisk = [
    DataSpec("/ekofisk/", "Ekofisk", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/ekofisk/", "Ekofisk", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/ekofisk/", "Ekofisk", "WL3", "average_water_level_ref_LAT"),
    DataSpec("/ekofisk/", "Ekofisk", "WL4", "average_water_level_ref_LAT"),
]

list_specs_ekofiskL = [
    DataSpec("/ekofiskL/", "EKO-L", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/ekofiskL/", "EKO-L", "WL3", "average_water_level_ref_LAT"),
    DataSpec("/ekofiskL/", "EKO-L", "WL4", "average_water_level_ref_LAT"),
]

# heidrun has some data
list_specs_heidrun = [
    DataSpec("/heidrun/", "HDR", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/heidrun/", "HDR", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/heidrun/", "HDR", "WL3", "average_water_level_ref_LAT"),
    DataSpec("/heidrun/", "Heidrun", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/heidrun/", "Heidrun", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/heidrun/", "Heidrun", "WL3", "average_water_level_ref_LAT"),
]

# draugen has some data
list_specs_draugen = [
    DataSpec("/draugen/", "Draugen", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/draugen/", "DRAUGEN", "WL1", "average_water_level_ref_LAT"),
]

# trollb has some data
list_specs_trollb = [
    DataSpec("/trollb/", "TROLL B", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/trollb/", "TROLL B", "WL2", "average_water_level_ref_LAT"),
]

# trollc has some data
list_specs_trollc = [
    DataSpec("/trollc/", "1000", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/trollc/", "1000", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/trollc/", "1000", "WL3", "average_water_level_ref_LAT"),
    DataSpec("/trollc/", "1032", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/trollc/", "1032", "WL2", "average_water_level_ref_LAT"),
    DataSpec("/trollc/", "1032", "WL3", "average_water_level_ref_LAT"),
]

# veslefrikka has some data
list_specs_veslefrikka = [
    DataSpec("/veslefrikka/", "1016", "WL1", "average_water_level_ref_LAT")
]

# veslefrikkb has some data
list_specs_veslefrikkb = [
    DataSpec("/veslefrikkb/", "1063", "WL1", "average_water_level_ref_LAT")
]

# sleipner has some data
list_specs_sleipner = [
    DataSpec("/sleipner/", "Sleipner A", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/sleipner/", "1017", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/sleipner/", "SleipnerA", "WL1", "average_water_level_ref_LAT")
]

# snorreb
list_specs_snorreb = [
    DataSpec("/snorreb/", "1039", "WL1", "average_water_level_ref_LAT"),
    DataSpec("/snorreb/", "1039", "WL2", "average_water_level_ref_LAT"),
]

############################################################
############################################################
############ Grab all the data we can from these stations
############################################################
############################################################

# first, we choose the time range of the dataset:
datetime_start = datetime.datetime(1999, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
datetime_end = datetime.datetime(2020, 6, 1, 0, 0, 0, tzinfo=pytz.utc)
datetime_resolution = datetime.timedelta(minutes=10)
timedelta_running_average_width = datetime.timedelta(days=10)
timedelta_slow_running_average_width = datetime.timedelta(days=20)

# then we need, for each of these datasets / specs, to parse, read, and put
# together the data from the corresponding d22 files. This takes quite a while.
if False:
    print("start populating the pkl data... This will take a while and needs lustre access")
    # the dataset is first in the form of a dict
    dict_dataset = {}

    dict_dataset["timestamps"] = list(datetime_range(datetime_start,
                                                     datetime_end,
                                                     datetime_resolution))

    # a few helper functions to avoid boiler plate code
    def build_dict_data(crrt_list_spec):
        data_extractor = DataExtractor()
        data_extractor.extract_available_data(crrt_list_spec)
        dict_time_series = data_extractor.data_as_time_series(datetime_start,
                                                              datetime_end,
                                                              datetime_resolution)

        ras(dict_time_series["timestamps"] == dict_dataset["timestamps"]) 

        for crrt_spec in crrt_list_spec:
            dict_dataset[crrt_spec] = dict_time_series[crrt_spec]

    build_dict_data(list_specs_heimdal)

    build_dict_data(list_specs_ekofisk)

    build_dict_data(list_specs_ekofiskL)

    build_dict_data(list_specs_heidrun)

    build_dict_data(list_specs_draugen)

    build_dict_data(list_specs_trollb)

    build_dict_data(list_specs_trollc)

    build_dict_data(list_specs_veslefrikka)

    build_dict_data(list_specs_veslefrikkb)

    build_dict_data(list_specs_sleipner)

    build_dict_data(list_specs_snorreb)

    # save

    with open(path_WL_data, "wb") as fh:
        pickle.dump(dict_dataset, fh)

# at this stage, we have created the pkl dict with the dataset and it is available for loading
print("start loading pkl data...")

with open(path_WL_data, "rb") as fh:
    dict_dataset = pickle.load(fh)

print("... done loading pkl data!")

############################################################
############################################################
########## Now we can do a first step of automated cleaning
############################################################
############################################################

# this dataset is full of outliers, bad values, etc. We need to do some serious cleaning on it.
# for this we are going to do several things:
# - interpolate the nan data that are surrounded by good data. This will take care of NaNs that come from
#   trying to read a 30 min period signal at a 10 min period, which is the case with old parts of the time
#   series.
# - remove outliers using a hampel filter, at several time scales: both time scale of several days and several
#   months.
# we do this for all the specs, and it will take quite a while.
if False:
    print("perform simple data interpolation and outlier detection")
    print("this takes a while and the result will be dumped as pkl of dict")

    dict_dataset_interpolated_outlier_removed = {}
    dict_dataset_interpolated_outlier_removed["timestamps"] = dict_dataset["timestamps"]

    for crrt_entry in dict_dataset:
        if isinstance(crrt_entry, DataSpec):
            print("process spec {}".format(crrt_entry))
            crrt_data = np.array(dict_dataset[crrt_entry])
            array_processed, mask = outlier_dropout_processing(crrt_data, datetime_resolution)

            dict_dataset_interpolated_outlier_removed[crrt_entry] = {}
            dict_dataset_interpolated_outlier_removed[crrt_entry]["array_processed"] = array_processed
            dict_dataset_interpolated_outlier_removed[crrt_entry]["mask_modified"] = mask

    # dump as pickle
    with open(path_interpolated_outlier_removed_data, "wb") as fh:
        pickle.dump(dict_dataset_interpolated_outlier_removed, fh)

# at this time, we have an interpolated, outlier-cleaned dataset
print("start loading pkl data interpolated outlier removed...")
with open(path_interpolated_outlier_removed_data, "rb") as fh:
    dict_dataset_interpolated_outlier_removed = pickle.load(fh)

print("... done loading interpolated outlier removed")

############################################################
############################################################
########### Now perform "by hand" operations: choose which
########### instrument to use on each station, how to clip
########### paste, remove trends, etc.
############################################################
############################################################

# from here on, this is really some "scratchbook" and will be heavily dependent on which sort of data blocks,
# missing data, etc, each application is looking at.

# what we need to do is go by hand through each individual station, in order to:
# - fix the jumps: the sensors are moved up and down in a non consistent way over their life, this must be accounted for
# - decide how to "merge" data from different specs when several instruments are present for a given location

# first, a few helper functions

def show_curves_for_list_specs(list_specs, show_raw=True, show_averaged=False):
    print("start plotting curves for list of specs...")

    plt.figure()
    for crrt_spec in list_specs:
        if show_raw:
            plt.plot(dict_dataset["timestamps"], dict_dataset[crrt_spec], label=str(crrt_spec))

        plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
                 dict_dataset_interpolated_outlier_removed[crrt_spec]["array_processed"],
                 label=str(crrt_spec) + " interpolated outlier removed")

        if show_averaged:
            plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
                     running_average(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                     ["array_processed"],
                                     datetime_resolution,
                                     timedelta_running_average_width),
                     label=str(crrt_spec) + " averaged")
            plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
                     running_average(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                     ["array_processed"],
                                     datetime_resolution,
                                     timedelta_slow_running_average_width),
                     label=str(crrt_spec) + " slow averaged")
    plt.legend(loc="lower right")
    plt.show()

def fix_jumps_in_signal(list_datetimes_in,
                        list_signals_in, jump_bounds_datetimes, mean_values, data_to_use,
                        datetime_resolution, take_away_excessive_values=True,
                        excessive_threshold=10):
    """fix jumps the signal, both in the source of the signal and in the mean level of the signal.
    - list_datetimes_in: the time base, common to all signals, a list of datetime.datetime elements
    - list_signals_in: the list of signals to use to build the final signal
    - jump_bounds_datetimes: the list of datetimes on which there is a jump of mean level or which signal
        to use.
    - mean_values: the means values to remove between jumps
    - data_to_use: the index of the signal in list_signals_in to use between jumps.

    the jumps are bounds, the mean_values and data_to_use are values between the bounds, i.e. 1 shorted.
    """
    ras(isinstance(list_datetimes_in, list))
    ras(isinstance(list_signals_in, list))
    for signal_in in list_signals_in:
        ras(isinstance(signal_in, np.ndarray))
    ras(len(jump_bounds_datetimes) == len(mean_values) + 1)
    ras(isinstance(jump_bounds_datetimes, list))
    for crrt_datetime in jump_bounds_datetimes:
        assert_is_utc_datetime(crrt_datetime)

    ras(isinstance(mean_values, list))

    fixed_signal = np.empty((len(list_datetimes_in),))
    fixed_signal[:] = np.nan

    for crrt_low_bound, crrt_high_bound, crrt_mean, crrt_data_to_use in \
            zip(jump_bounds_datetimes[:-1], jump_bounds_datetimes[1:], mean_values, data_to_use):
        for (crrt_ind, crrt_datetime) in enumerate(list_datetimes_in):
            if crrt_datetime >= crrt_low_bound and crrt_datetime < crrt_high_bound:
                fixed_signal[crrt_ind] = list_signals_in[crrt_data_to_use][crrt_ind] - crrt_mean

    if take_away_excessive_values:
        # excessive values can safely be ignored
        list_index_invalid = np.where(np.abs(fixed_signal) > excessive_threshold)
        fixed_signal[list_index_invalid] = np.nan

    # take one last outlier detection run for points at the edge of the jumps
    (fixed_signal, _) = hampel(fixed_signal,
                               int(datetime.timedelta(days=2.0) / datetime_resolution),
                               t0=5,
                               use_tqdm=True)

    return(fixed_signal)


def compensate_sinking_on_segments(list_datetimes_in, signal_in, limit_datetime_segments, show_fit=True):
    """compensate linearly for the sinking of the station, individually on separate data segments (for example because
    the sensors are being moved around).
    - list_datetimes_in: the time base
    - signal_in: the signal to detrend for sinking
    - limit_datetime_segments: the limits of the different segments, corresponding to when the sensors are moved.
    """

    detrended_signal = np.copy(signal_in)

    for (crrt_min_datetime, crrt_max_datetime) in zip(limit_datetime_segments[:-1], limit_datetime_segments[1:]):
        # find the indexes to consider
        crrt_start_index = np.where(np.array(list_datetimes_in) > crrt_min_datetime)[0][0]
        crrt_largest_search = np.where(np.array(list_datetimes_in) > crrt_max_datetime)[0]
        if crrt_largest_search.shape == (0,):
            crrt_end_index = len(list_datetimes_in)
        else:
            crrt_end_index = crrt_largest_search[0]

        # fit linear curve to the slow average
        crrt_array = signal_in[crrt_start_index:crrt_end_index]
        crrt_indexes = np.arange(0, crrt_end_index-crrt_start_index)
        valid_indexes = np.isfinite(crrt_array)

        linear_fit_coef = np.polyfit(crrt_indexes[valid_indexes], crrt_array[valid_indexes], 1)
        poly1d_fn = np.poly1d(linear_fit_coef) 
        detrending = poly1d_fn(crrt_indexes)

        # compensate using the linear fit data
        detrended_signal[crrt_start_index:crrt_end_index] = crrt_array - detrending

        if show_fit:
            plt.figure()
            plt.plot(list_datetimes_in[crrt_start_index: crrt_end_index],
                     signal_in[crrt_start_index:crrt_end_index], label="initial")
            plt.plot(list_datetimes_in[crrt_start_index: crrt_end_index],
                     detrended_signal[crrt_start_index:crrt_end_index], label="detrended")
            plt.legend(loc="lower right")
            plt.show()

    (detrended_signal, _) = hampel(detrended_signal,
                                   int(datetime.timedelta(days=10.0) / datetime_resolution),
                                   t0=4,
                                   use_tqdm=True)

    return detrended_signal


# taking care of one station at a time
dict_dataset_interpolated_no_outlier_no_jump = {}
dict_dataset_interpolated_no_outlier_no_jump["timestamps"] = dict_dataset["timestamps"] 

if False:  # fix Heimdal
    crrt_station = "heimdal"
    crrt_specs = list_specs_heimdal

    show_curves_for_list_specs(crrt_specs)

    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    jump_bounds_datetimes_heimdal = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2010, month=10, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2010, month=12, day=10, hour=17, tzinfo=pytz.utc),
        datetime.datetime(year=2011, month=1, day=18, hour=14, tzinfo=pytz.utc),
        datetime.datetime(year=2014, month=3, day=21, hour=13, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc)
    ]

    mean_values_heimdal = [
        0,
        -57.5,
        15.3,
        -57.2,
        0, 
    ]

    data_to_use_heimdal = [
        0,
        0,
        0,
        0,
        0,
    ]

    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_heimdal,
                                mean_values_heimdal,
                                data_to_use_heimdal,
                                datetime_resolution)

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)


if False: # fix ekofisk
    crrt_station = "ekofisk"
    crrt_specs = list_specs_ekofisk + list_specs_ekofiskL

    # show_curves_for_list_specs(crrt_specs)

    # start by selecting the "right" sensor on each segment
    # these are all the signals we can use
    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    # these are the points in time when we want to change which signal to use
    jump_bounds_datetimes_ekofisk = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2002, month=2, day=25, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=9, day=9, tzinfo=pytz.utc),
        datetime.datetime(year=2008, month=7, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2012, month=8, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2014, month=5, day=21, tzinfo=pytz.utc),
        datetime.datetime(year=2015, month=3, day=9, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # these are mean values to subtract, not used here
    mean_values_ekofisk = [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ]

    # these are the specs to use on each of the previous segments
    data_to_use_ekofisk = [
        crrt_specs.index(DataSpec("/ekofisk/", "Ekofisk", "WL1", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofisk/", "Ekofisk", "WL2", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofisk/", "Ekofisk", "WL3", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofisk/", "Ekofisk", "WL2", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofisk/", "Ekofisk", "WL3", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofiskL/", "EKO-L", "WL2", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/ekofiskL/", "EKO-L", "WL3", "average_water_level_ref_LAT")), 
    ]

    # peform the "cut and paste": using different sensors over time following the previous specs,
    # taking care of removing outliers
    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_ekofisk,
                                mean_values_ekofisk,
                                data_to_use_ekofisk,
                                datetime_resolution)

    # now we have a "fixed" signal, where we use always the best sensor
    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()
    plt.show()

    # now take care of bottom sinking
    # the problem is that the sensors are clearly moved up and down now and then; so, we have to decide
    # over which segments the sensors are still, and remove the mean linear trend on each segment to compensate
    # for the sinking

    # the time limits when we think the sensors have been moved
    limit_datetime_segments_moved_sensor = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2001, month=10, day=5, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=7, day=18, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=9, day=10, tzinfo=pytz.utc),
        datetime.datetime(year=2005, month=9, day=16, tzinfo=pytz.utc),
        datetime.datetime(year=2009, month=6, day=26, tzinfo=pytz.utc),
        datetime.datetime(year=2010, month=3, day=16, tzinfo=pytz.utc),
        datetime.datetime(year=2017, month=8, day=24, tzinfo=pytz.utc),
        datetime.datetime(year=2017, month=10, day=2, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # detrend on each of the segments
    fixed_sinking_removed = \
        compensate_sinking_on_segments(dict_dataset_interpolated_outlier_removed["timestamps"],
                                       fixed,
                                       limit_datetime_segments=limit_datetime_segments_moved_sensor
                                       )

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed_sinking_removed

    # the final result
    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)

if False:
    crrt_station = "heidrun"
    crrt_specs = list_specs_heidrun

    # show_curves_for_list_specs(crrt_specs)

    # start by selecting the "right" sensor on each segment
    # these are all the signals we can use
    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    jump_bounds_datetimes_heidrun = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2003, month=1, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=8, day=1, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # these are mean values to subtract, not used here
    mean_values_heidrun = [
        0,
        0,
        0,
    ]

    # these are the specs to use on each of the previous segments
    data_to_use_heidrun = [
        crrt_specs.index(DataSpec("/heidrun/", "Heidrun", "WL1", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/heidrun/", "Heidrun", "WL2", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/heidrun/", "HDR", "WL2", "average_water_level_ref_LAT")), 
    ]

    # peform the "cut and paste": using different sensors over time following the previous specs,
    # taking care of removing outliers
    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_heidrun,
                                mean_values_heidrun,
                                data_to_use_heidrun,
                                datetime_resolution,
                                take_away_excessive_values=False)

    # now we have a "fixed" signal, where we use always the best sensor
    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()
    plt.show()

    # now take care of sensors jumps
    # the time limits when we think the sensors have been moved
    limit_datetime_segments_moved_sensor = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=1999, month=6, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2001, month=2, day=1, hour=20, minute=30, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=1, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2003, month=8, day=1, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # detrend on each of the segments
    fixed_sinking_removed = \
        compensate_sinking_on_segments(dict_dataset_interpolated_outlier_removed["timestamps"],
                                       fixed,
                                       limit_datetime_segments=limit_datetime_segments_moved_sensor
                                       )

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed_sinking_removed

    # the final result
    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)

if False:
    crrt_station = "draugen"
    crrt_specs = list_specs_draugen

    show_curves_for_list_specs(crrt_specs)

    # start by selecting the "right" sensor on each segment
    # these are all the signals we can use
    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    jump_bounds_datetimes_draugen = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2008, month=6, day=15, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # these are mean values to subtract, not used here
    mean_values_draugen = [
        2.5,
        2.6,
    ]

    # these are the specs to use on each of the previous segments
    data_to_use_draugen = [
        crrt_specs.index(DataSpec("/draugen/", "Draugen", "WL1", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/draugen/", "DRAUGEN", "WL1", "average_water_level_ref_LAT")), 
    ]

    # peform the "cut and paste": using different sensors over time following the previous specs,
    # taking care of removing outliers
    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_draugen,
                                mean_values_draugen,
                                data_to_use_draugen,
                                datetime_resolution,
                                take_away_excessive_values=True,
                                excessive_threshold=4.5)

    # now we have a "fixed" signal, where we use always the best sensor
    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()
    plt.show()

    # now take care of sensors jumps
    # the time limits when we think the sensors have been moved
    limit_datetime_segments_moved_sensor = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2002, month=7, day=1, tzinfo=pytz.utc),
        datetime.datetime(year=2008, month=6, day=15, tzinfo=pytz.utc),
        datetime.datetime(year=2018, month=10, day=26, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # detrend on each of the segments
    fixed_sinking_removed = \
        compensate_sinking_on_segments(dict_dataset_interpolated_outlier_removed["timestamps"],
                                       fixed,
                                       limit_datetime_segments=limit_datetime_segments_moved_sensor
                                       )

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed_sinking_removed

    # the final result
    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)


if True:
    crrt_station = "troll"
    crrt_specs = list_specs_trollb + list_specs_trollc

    show_curves_for_list_specs(crrt_specs)

    # the data are clearly unusable

if False:
    crrt_station = "veslefrikk"
    crrt_specs = list_specs_veslefrikka + list_specs_veslefrikkb

    # show_curves_for_list_specs(crrt_specs)

    # these are excellent data, little to no problem at all

    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    jump_bounds_datetimes_veslefrikk = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # these are mean values to subtract, not used here
    mean_values_veslefrikk = [
        0,
    ]

    # these are the specs to use on each of the previous segments
    data_to_use_veslefrikk = [
        crrt_specs.index(DataSpec("/veslefrikkb/", "1063", "WL1", "average_water_level_ref_LAT")), 
    ]

    # peform the "cut and paste": using different sensors over time following the previous specs,
    # taking care of removing outliers
    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_veslefrikk,
                                mean_values_veslefrikk,
                                data_to_use_veslefrikk,
                                datetime_resolution,
                                take_away_excessive_values=False)

    # now we have a "fixed" signal, where we use always the best sensor
    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()
    plt.show()

    # now take care of sensors jumps
    # the time limits when we think the sensors have been moved
    limit_datetime_segments_moved_sensor = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # detrend on each of the segments
    fixed_sinking_removed = \
        compensate_sinking_on_segments(dict_dataset_interpolated_outlier_removed["timestamps"],
                                       fixed,
                                       limit_datetime_segments=limit_datetime_segments_moved_sensor
                                       )

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed_sinking_removed

    # the final result
    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)

if False:
    crrt_station = "sleipner"
    crrt_specs = list_specs_sleipner

    # show_curves_for_list_specs(crrt_specs)

    list_signals_in = [np.array(dict_dataset_interpolated_outlier_removed[crrt_spec]
                                ["array_processed"]) for crrt_spec in crrt_specs]

    jump_bounds_datetimes_sleipner = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2014, month=11, day=10, hour=9, minute=45, tzinfo=pytz.utc),
        datetime.datetime(year=2014, month=11, day=24, hour=12, minute=23, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # these are mean values to subtract, not used here
    mean_values_sleipner = [
        0,
        0,
        0,
    ]

    # these are the specs to use on each of the previous segments
    data_to_use_sleipner = [
        crrt_specs.index(DataSpec("/sleipner/", "Sleipner A", "WL1", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/sleipner/", "1017", "WL1", "average_water_level_ref_LAT")), 
        crrt_specs.index(DataSpec("/sleipner/", "SleipnerA", "WL1", "average_water_level_ref_LAT")), 
    ]

    # peform the "cut and paste": using different sensors over time following the previous specs,
    # taking care of removing outliers
    fixed = fix_jumps_in_signal(dict_dataset_interpolated_outlier_removed["timestamps"],
                                list_signals_in,
                                jump_bounds_datetimes_sleipner,
                                mean_values_sleipner,
                                data_to_use_sleipner,
                                datetime_resolution,
                                take_away_excessive_values=False)

    # now we have a "fixed" signal, where we use always the best sensor
    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed

    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()
    plt.show()

    # now take care of sensors jumps
    # the time limits when we think the sensors have been moved
    limit_datetime_segments_moved_sensor = [
        datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc),
        # start jumps
        datetime.datetime(year=2014, month=11, day=10, hour=9, minute=45, tzinfo=pytz.utc),
        datetime.datetime(year=2014, month=11, day=24, hour=12, minute=23, tzinfo=pytz.utc),
        # end jumps
        datetime.datetime(year=2900, month=1, day=1, tzinfo=pytz.utc),
    ]

    # detrend on each of the segments
    fixed_sinking_removed = \
        compensate_sinking_on_segments(dict_dataset_interpolated_outlier_removed["timestamps"],
                                       fixed,
                                       limit_datetime_segments=limit_datetime_segments_moved_sensor
                                       )

    dict_dataset_interpolated_no_outlier_no_jump[crrt_station] = fixed_sinking_removed

    # the final result
    plt.figure()
    plt.plot(dict_dataset_interpolated_outlier_removed["timestamps"],
             dict_dataset_interpolated_no_outlier_no_jump[crrt_station],
             label="fixed signal {}".format(crrt_station))
    plt.legend()

    show_curves_for_list_specs(crrt_specs)

    with open("dict_fixed_{}.pkl".format(crrt_station), "wb") as fh:
        pickle.dump(dict_dataset_interpolated_no_outlier_no_jump, fh)

if False:
    # the WL instruments are clearly not working
    crrt_station = "snorreb"
    crrt_specs = list_specs_snorreb

    show_curves_for_list_specs(crrt_specs)


############################################################
############################################################
############ assemble all the stations in a nc4 file
############################################################
############################################################

if False:
    # we are now ready to export as netCDF
    nc4_path = "./WL_oil_platforms.nc4"

    list_usable_platforms = [
        "draugen",
        "ekofisk",
        "heidrun",
        "heimdal",
        "sleipner",
        "veslefrikk",
    ]

    fill_value = 1.0e37

    print("start writing nc4 dataset...")

    with nc4.Dataset(nc4_path, "w", format="NETCDF4") as nc4_fh: 
        nc4_fh.set_auto_mask(False)

        description_string = "Water Level (WL) dataset from the d22 data provided " +\
                            "by the Norwegian oil platforms. These are cleaned and pre " +\
                            "processed data from " +\
                            "/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE " +\
                            "generated on {} ".format(datetime.datetime.now().isoformat()[:10]) +\
                            "using code from https://gitlab.met.no/jeanr/d22_data_format/-/tree/master/WL_data " +\
                            "in all the following, units are m, and all timestamps are UTC."

        nc4_fh.Conventions = "CF-X.X"
        nc4_fh.title = "water level from d22 WL blocks of Norwegian oil platforms"
        nc4_fh.description = description_string
        nc4_fh.institution = "IT department, Norwegian Meteorological Institute, using d22 WL data from oil platforms"
        nc4_fh.Contact = "jeanr@met.no"

        _ = nc4_fh.createDimension('station', len(list_usable_platforms))
        number_of_time_entries = len(dict_dataset["timestamps"])
        _ = nc4_fh.createDimension('time', number_of_time_entries)

        stationid = nc4_fh.createVariable("stationid", str, ('station'))
        latitude = nc4_fh.createVariable('latitude', 'f4', ('station'))
        longitude = nc4_fh.createVariable('longitude', 'f4', ('station'))
        timestamps = nc4_fh.createVariable('timestamps', 'i8', ('time'))
        observation = nc4_fh.createVariable('observation', 'f4', ('station', 'time'))

        stationid.description = "unique ID string of each station"
        stationid.units = "none"

        latitude.description = "latitude of each station"
        latitude.units = "degree North"

        longitude.description = "longitude of each station"
        longitude.units = "degree East"

        timestamps.description = "common time base for all data"
        timestamps.units = "POSIX timestamp"

        observation.description = "water level observation at each station over the time base"
        observation.units = "m, fill value: 1.0e37"
        observation.standard_name = "observed_sea_surface_height"

        timestamps_vector = [crrt_datetime.timestamp()
                            for crrt_datetime in dict_dataset["timestamps"]]

        timestamps[:] = np.array(timestamps_vector)

        for ind, crrt_station_id in enumerate(list_usable_platforms):
            stationid[ind] = crrt_station_id

            latitude[ind] = dict_name_to_position[crrt_station_id][0]
            longitude[ind] = dict_name_to_position[crrt_station_id][1]

            crrt_pkl_dict = "dict_fixed_{}.pkl".format(crrt_station_id)
            with open(crrt_pkl_dict, "rb") as fh:
                dict_read_pkl_data = pickle.load(fh)
                np_observations = dict_read_pkl_data[crrt_station_id]
                np_observations[np.where(np.isnan(np_observations))] = fill_value

            observation[ind, :] = np_observations

    print("...done")
