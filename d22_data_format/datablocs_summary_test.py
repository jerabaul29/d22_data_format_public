from d22_data_format.datablocs_summary import generate_datablocks_overview_dict,\
    generate_stats_on_dict_metadata

from d22_data_format.helpers.load_test_data import get_parsed_correct_data, \
    path_to_test_data


def test_1():
    dict_metadata = generate_datablocks_overview_dict(path_root_data=path_to_test_data() + "/sample_AASTA/",
                                                      list_stations_subfolders=["aastahansteen"])
    dict_pure_metadata = generate_stats_on_dict_metadata(dict_metadata, print_info=True)

    dict_parsed = get_parsed_correct_data("datablocks_pure_metadata_AASTA.py")

    assert dict_pure_metadata == dict_parsed
