# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class FileSystemReaderWriter(object):

    def __init__(self, path):
        assert path is not None
        self._path = path

    def read_lines(self):
        with open(self._path, "r") as f:
            return f.readlines()

    def overwrite_with_line(self, line):
        with open(self._path, "w") as f:
            f.writelines(line)
