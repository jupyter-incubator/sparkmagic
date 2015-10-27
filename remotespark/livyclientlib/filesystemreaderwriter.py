# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import os

from .utils import expand_path


class FileSystemReaderWriter(object):

    def __init__(self, path):
        assert path is not None
        self._path = expand_path(path)

    def read_lines(self):
        if os.path.isfile(self._path):
            with open(self._path, "r+") as f:
                return f.readlines()
        else:
            return ""

    def overwrite_with_line(self, line):
        with open(self._path, "w+") as f:
            f.writelines(line)
