"""A few tools to explore the d22 tree structure."""

import logging

from pathlib import Path

from d22_data_format.helpers.folders_navigation import get_sorted_subfolders
from d22_data_format.helpers.raise_assert import ras


def d22_files_in_dir_generator(path_root_data=None, list_stations_subfolders=None):
    for crrt_folder in d22_global_dirs_generator(path_root_data, list_stations_subfolders):
        files = sorted([x for x in Path(crrt_folder).glob("*") if x.is_file()])
        for crrt_file in files:
            yield crrt_file

def d22_global_dirs_generator(path_root_data=None, list_stations_subfolders=None):
    """A generator for the global dirs where d22 data are contained.
    Input:
        - path_root_data: where to look at the data. None (default) is the right location
        on lustreB.
        - list_stations_subfolders: if only some stations should be considered, put the
        list of corresponding folders there. Defaults to None, all stations are
        investigated.
    Output:
        - The list of folders containing the data, as a generator.
    Note: This is a generator.
    """
    if path_root_data is None:
        path_root_data = "/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/"

    if list_stations_subfolders is None:
        stations_subfolders = get_sorted_subfolders(path_root_data)
    else:
        stations_subfolders = []
        for crrt_folder in list_stations_subfolders:
            stations_subfolders.append(path_root_data + "/" + crrt_folder + "/")
            crrt_path = stations_subfolders[-1]
            ras(Path(crrt_path).exists(), "path does not exist: {}".format(crrt_path))

    for crrt_station_path in stations_subfolders:
        logging.info("explore station path {}".format(crrt_station_path))

        d22_path = crrt_station_path + "/d22/"

        if not Path(d22_path).is_dir():
            logging.warning("seems like {} has no d22!".format(crrt_station_path))
            continue

        yield from d22_station_generator(d22_path)


def d22_station_generator(path_station_d22):
    """Lists all folders in a d22 tree structure, ordered by time.

    This is a generator."""
    logging.info("station generator call from {}".format(path_station_d22))

    list_subfolders = list(get_sorted_subfolders(path_station_d22))

    if not list_subfolders:
        logging.info("no subfolder, yield")
        yield path_station_d22

    else:
        for crrt_subfolder in list_subfolders:
            logging.info("process subfolder {}".format(crrt_subfolder))
            yield from d22_station_generator(crrt_subfolder + "/")
