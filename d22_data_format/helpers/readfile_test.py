import tempfile

from d22_data_format.helpers.readfile import FileLinesYielder


def test_file_lines_yielder_class():
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpfile = tmpdirname + "/test_1.txt"

        with open(tmpfile, "w", encoding="ascii", errors="strict") as crrt_fh:
            crrt_fh.write("line1\nline2\nThis is line 3!")

        file_lines_yielder_instance = FileLinesYielder()
        result = file_lines_yielder_instance.file_lines_yielder(tmpfile)

        correct_result = ["line1\n",
                          "line2\n",
                          "This is line 3!"]

        assert list(result) == correct_result
