import logging

import os
from pathlib import Path

import pickle

from d22_data_format.datablocs_summary import generate_datablocks_overview_dict, \
    generate_stats_on_dict_metadata, show_summary_blocks_across_stations, \
    show_summary_blocks_one_station

logging.basicConfig(level=logging.WARNING)

if False:
    dict_metadata = generate_datablocks_overview_dict()
    dict_pure_metadata = generate_stats_on_dict_metadata(dict_metadata, print_info=True)

    if Path("dict_pure_metadata.pkl").is_file():
        os.remove("dict_pure_metadata.pkl")

    with open('dict_pure_metadata.pkl', 'wb') as fh:
        pickle.dump(dict_pure_metadata, fh, protocol=pickle.HIGHEST_PROTOCOL)

with open('dict_pure_metadata.pkl', "rb") as fh:
    dict_pure_metadata = pickle.load(fh)

show_summary_blocks_one_station(dict_pure_metadata, "Heimdal")

show_summary_blocks_across_stations(dict_pure_metadata, list_block_prefixes=["WL"])
