import logging
import gzip


class FileLinesYielder():
    def __init__(self, nbr_context_lines=5):
        self.nbr_context_lines = nbr_context_lines
        self.list_lines = self.nbr_context_lines * ["!!EMPTY_CONTEXT_LINE!!"]

    def file_lines_yielder(self, path_to_file, encoding="ascii", errors="strict", automatic_gzip_recognition=True):
        # NOTE: this starts to be a mess, because the encoding / errors handling are different between gz and
        # the open command. But but, working for now...
        self.path_to_file = path_to_file

        if automatic_gzip_recognition and str(self.path_to_file)[-3:] == ".gz":
            open_command = gzip.open
            flags_open = "rb"
            decode_encoding = encoding
            encoding = None
            errors = None

            def decode(input):
                return input.decode(decode_encoding)
        else:
            open_command = open
            flags_open = "r"

            def decode(input):
                return input

        with open_command(str(self.path_to_file), flags_open, encoding=encoding, errors=errors) as crrt_fh:
            for self.crrt_ind, self.crrt_line in enumerate(crrt_fh):
                self.to_yield = decode(self.crrt_line)

                logging.info("line {}: {} decoded: {}".format(self.crrt_ind, self.crrt_line, self.to_yield))

                self.list_lines.pop(0)
                self.list_lines.append(self.to_yield)

                yield self.to_yield
