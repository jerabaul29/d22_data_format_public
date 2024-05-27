import logging

# enum is best (std lib), but some features are 3.6 and higher only
try:
    from enum import Enum, unique, auto
except:
    from aenum import Enum, unique, auto

from pathlib import Path

import pprint

import datetime
import pytz

from d22_data_format.helpers.readfile import FileLinesYielder
from d22_data_format.helpers.datetimes import assert_is_utc_datetime
from d22_data_format.helpers.raise_assert import ras
from d22_data_format.helpers.load_test_data import path_to_test_data
from d22_data_format.helpers.generator_oneback import GeneratorOneback

d22_encoding = "latin-1"


@unique
class d22_parser_status(Enum):
    OUTSIDE_PACKAGE = auto()
    OUTSIDE_BLOCK = auto()
    GRACIOUS_END_OF_FILE = auto()
    ERROR_END_OF_FILE = auto()


class D22Parser():
    def __init__(self, path_to_d22_file, automatic_gzip_recognition=True):
        ras(Path(path_to_d22_file).is_file())
        # TODO: if the file is a gz, unzip first.
        self.path_to_d22_file = path_to_d22_file
        self.parser_state = d22_parser_status.OUTSIDE_PACKAGE
        self.file_lines_yielder_instance = FileLinesYielder()
        file_lines_yielder = self.file_lines_yielder_instance.file_lines_yielder
        self.line_yielder = GeneratorOneback(file_lines_yielder(path_to_d22_file,
                                                                automatic_gzip_recognition=automatic_gzip_recognition,
                                                                encoding=d22_encoding))
        self.automatic_gzip_recognition = automatic_gzip_recognition
        self.dict_result = {}

    def perform_parsing(self):
        """Parse the whole file content into a dict."""
        while self.parser_state != d22_parser_status.GRACIOUS_END_OF_FILE:
            self.parse_once_more()

        return self.dict_result

    def parse_once_more(self):
        """Perform one more step of parsing."""
        if self.parser_state == d22_parser_status.OUTSIDE_PACKAGE:
            self.act_from_outside_package()

        elif self.parser_state == d22_parser_status.OUTSIDE_BLOCK:
            self.act_from_outside_block()

        elif (self.parser_state == d22_parser_status.GRACIOUS_END_OF_FILE
              or self.parser_state == d22_parser_status.ERROR_END_OF_FILE):
            raise ValueError("We do not expect to call this function once EOF is reached!")

    def obtain_next_line(self):
        """Get one more line of the file, or exit either gracefully or with
        error."""
        try:
            crrt_line = next(self.line_yielder)

        except StopIteration:
            crrt_line = None
            if self.parser_state == d22_parser_status.OUTSIDE_PACKAGE:
                logging.info("Gracefully end parsing")
                self.parser_state = d22_parser_status.GRACIOUS_END_OF_FILE
            else:
                self.parser_state = d22_parser_status.ERROR_END_OF_FILE
                raise ValueError("Hit end of file, but we are not outside of a data package yet!")

        return(crrt_line)

    def act_from_outside_package(self):
        """Perform one step of parsing when outside a data pacakge."""

        # we are outside a package; we will now:
        # 1) find the next package content, or reach end of file
        # 2) parse the package info
        # 3) create the package entry in dict
        # 4) update the status

        logging.info("parse from outside package")

        # 1
        while True:
            crrt_line = self.obtain_next_line()

            if self.parser_state == d22_parser_status.GRACIOUS_END_OF_FILE:
                break

            if crrt_line == "!!!!\n" or crrt_line == "!!!!\r\n":
                logging.info("found start of package")
                data_format_line = self.obtain_next_line()
                crrt_station_name = self.obtain_next_line()
                utc_date_line = self.obtain_next_line()
                utc_time_line = self.obtain_next_line()
                break

        # 2
        if self.parser_state == d22_parser_status.OUTSIDE_PACKAGE:
            logging.info("analyze package header")

            ras(data_format_line[0:5] == "DF022" or data_format_line[0:6] == "DF-022" or
                data_format_line[0:9] == "DF-015/01")

            self.crrt_station_name = crrt_station_name[:-2].rstrip()

            crrt_day = int(utc_date_line[0:2])
            crrt_month = int(utc_date_line[3:5])
            crrt_year = int(utc_date_line[6:10])

            crrt_hour = int(utc_time_line[0:2])
            crrt_minute = int(utc_time_line[3:5])
            crrt_second = 0

            self.crrt_utc_datetime = \
                datetime.datetime(year=crrt_year, month=crrt_month, day=crrt_day,
                                  hour=crrt_hour, minute=crrt_minute, second=crrt_second,
                                  tzinfo=pytz.utc)

            assert_is_utc_datetime(self.crrt_utc_datetime)

            # 3
            if self.crrt_station_name not in self.dict_result:
                self.dict_result[self.crrt_station_name] = {}

            if self.crrt_utc_datetime in self.dict_result[self.crrt_station_name]:
                logging.warning("meeting time {} again; this is unexpected; ".format(self.crrt_utc_datetime)
                                + "will attempt to parse though")
                self.warning_with_current_details()
            else:
                self.dict_result[self.crrt_station_name][self.crrt_utc_datetime] = {}

            # 4
            self.parser_state = d22_parser_status.OUTSIDE_BLOCK

    def act_from_outside_block(self):
        """Perform one step of parsing when outside a data block, but inside a
        data package."""

        # we are outside a block, but inside a package; we will now;
        # 1) either find block start or end of package
        # 2) if block start, parse block and add entry
        # 3) update status

        logging.info("parse from outside block")

        block_title_line = self.obtain_next_line()

        # 1
        if block_title_line == "\f$$$$$$$\n" or block_title_line == "\f$$$$$$$\r\n":
            logging.info("found end of package")
            # 3 a
            self.parser_state = d22_parser_status.OUTSIDE_PACKAGE
        elif block_title_line == "!!!!\n" or block_title_line == "!!!!\r\n":
            logging.warning("found an unexpected start of package, probably transmission was cut!")
            self.warning_with_current_details()
            self.parser_state = d22_parser_status.OUTSIDE_PACKAGE
        elif "-" in block_title_line:
            # 2
            logging.info("found start of block")
            ras(block_title_line[0]) == "\f"
            block_title_line = block_title_line[1:]

            ras("-" in block_title_line)
            position_delimiter = block_title_line.find("-")
            ras(position_delimiter != -1)
            ras(block_title_line[-1:] == "\n")

            block_title = block_title_line[0:position_delimiter]
            block_size = int(block_title_line[position_delimiter+1:-1])

            logging.info("block_title: {}".format(block_title))
            logging.info("block size: {}".format(block_size))

            log_block = True

            if block_title in self.dict_result[self.crrt_station_name][self.crrt_utc_datetime]:
                logging.warning("trying to insert again {} in {}/{}!".format(block_title,
                                                                             self.crrt_station_name,
                                                                             self.crrt_utc_datetime))
                logging.warning("will ignore this block")
                self.warning_with_current_details()
                log_block = False

            if log_block:
                self.dict_result[self.crrt_station_name][self.crrt_utc_datetime][block_title] = {}

                self.dict_result[self.crrt_station_name][self.crrt_utc_datetime][block_title]\
                    ["nbr_entries"] = block_size - 1

                self.dict_result[self.crrt_station_name][self.crrt_utc_datetime][block_title]\
                    ["list_entries"] = []

            valid_block = True

            for _ in range(block_size - 1):
                if valid_block:
                    crrt_entry = self.obtain_next_line()
                    ras(crrt_entry[-1:] == "\n")

                    try:
                        crrt_value = float(crrt_entry[:-1])
                    except:
                        logging.warning("cannot be converted to float!")
                        self.warning_with_current_details()
                        crrt_value = -999.88
                        valid_block = False
                        self.line_yielder.use_last_value()

                if log_block:
                    self.dict_result[self.crrt_station_name][self.crrt_utc_datetime][block_title]\
                        ['list_entries'].append(crrt_value)

            # 3 b
            self.parser_state = d22_parser_status.OUTSIDE_BLOCK
        else:
            logging.warning("looking for either end of package, or start of package, or start of block,")
            logging.warning("current line does not correspond to any of that!")
            logging.warning("this may be due to a corrupt line or block. The error can be a few lines up in the file.")
            self.warning_with_current_details()

    def warning_with_current_details(self):
        logging.warning("this error happened line {} file {}".format(self.file_lines_yielder_instance.crrt_ind,
                                                                     self.file_lines_yielder_instance.path_to_file))
        logging.warning("line content: {}".format(self.file_lines_yielder_instance.crrt_line))
        logging.warning("previous read lines are:")
        logging.warning(self.file_lines_yielder_instance.list_lines)


if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent=2).pprint

    path_to_d22_file = path_to_test_data("short_20130923.d22")
    d22_parser = D22Parser(path_to_d22_file=path_to_d22_file)
    dict_result = d22_parser.perform_parsing()

    pp(dict_result)
