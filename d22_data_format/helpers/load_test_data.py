import os
from pathlib import Path

from d22_data_format.helpers.raise_assert import ras

this_dir = os.path.dirname(os.path.abspath(__file__))
path_to_data = this_dir + "/../../example_data/"


def path_to_test_data(test_data_filename=""):
    full_path = path_to_data + test_data_filename
    ras(Path(full_path).exists())
    return(full_path)


def get_parsed_correct_data(parsed_data_pyname):
    full_path = path_to_data + parsed_data_pyname
    ras(Path(full_path).exists())

    # TODO: this is a bit of a hack, and confuses the linter, fixme
    exec(open(full_path).read(), globals())

    return(dict_parsed)
