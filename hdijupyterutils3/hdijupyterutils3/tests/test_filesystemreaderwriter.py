import os.path

from hdijupyterutils.filesystemreaderwriter import FileSystemReaderWriter


def test_read():
    path = "test"
    if os.path.isfile(path):
        os.remove(path)

    expected_lines = ["a\n", "b"]
    rw = FileSystemReaderWriter(path)
    with open("test", "w") as f:
        f.writelines(expected_lines)

    read_lines = rw.read_lines()
    assert expected_lines == read_lines

    os.remove(path)


def test_write_non_existent_file():
    path = "test"
    if os.path.isfile(path):
        os.remove(path)

    expected_line = "hi"

    rw = FileSystemReaderWriter(path)
    rw.overwrite_with_line(expected_line)

    with open("test", "r") as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0] == expected_line

    os.remove(path)


def test_overwrite_existent_file():
    path = "test"
    if os.path.isfile(path):
        os.remove(path)

    with open("test", "w") as f:
        f.writelines(["ab"])

    expected_line = "hi"

    rw = FileSystemReaderWriter(path)
    rw.overwrite_with_line(expected_line)

    with open("test", "r") as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0] == expected_line

    os.remove(path)
