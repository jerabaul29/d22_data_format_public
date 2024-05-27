from d22_data_format.exploration_tools import d22_global_dirs_generator
from d22_data_format.helpers.load_test_data import path_to_test_data


def test_explore_single_station():
    path_root_data = path_to_test_data() + "/dummy_d22_tree/"
    list_stations_subfolders = ["station_1"]

    list_folders_to_explore = list(d22_global_dirs_generator(path_root_data, list_stations_subfolders))

    correct_answer = [path_root_data + "/station_1//d22/2000/01/01/",
                      path_root_data + "/station_1//d22/2000/01/02/",
                      path_root_data + "/station_1//d22/2000/02/02/",
                      path_root_data + "/station_1//d22/2000/02/03/",
                      path_root_data + "/station_1//d22/2001/01/01/",
                      path_root_data + "/station_1//d22/2001/01/02/",
                      path_root_data + "/station_1//d22/2001/02/02/",
                      path_root_data + "/station_1//d22/2001/02/03/",
                      ]

    assert correct_answer == list_folders_to_explore

def test_explore_all_stations_in_dict():
    path_root_data = path_to_test_data() + "/dummy_d22_tree/"

    list_folders_to_explore = list(d22_global_dirs_generator(path_root_data))

    correct_answer = [path_root_data + "station_1/d22/2000/01/01/",
                      path_root_data + "station_1/d22/2000/01/02/",
                      path_root_data + "station_1/d22/2000/02/02/",
                      path_root_data + "station_1/d22/2000/02/03/",
                      path_root_data + "station_1/d22/2001/01/01/",
                      path_root_data + "station_1/d22/2001/01/02/",
                      path_root_data + "station_1/d22/2001/02/02/",
                      path_root_data + "station_1/d22/2001/02/03/",
                      path_root_data + "station_3/d22/1980/",
                      path_root_data + "station_3/d22/1981/",
                      ]

    assert correct_answer == list_folders_to_explore
