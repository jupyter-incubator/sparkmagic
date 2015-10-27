# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from datetime import datetime

from .utils import join_paths, get_magics_home_path


class Log(object):
    """Logger."""

    def __init__(self):
        try:
            magics_home_path = get_magics_home_path()
            self.path_to_serialize = join_paths(magics_home_path, "logs/log.txt")
        except KeyError:
            self.path_to_serialize = None

        self._mode = "normal"

    @property
    def mode(self):
        """One of: 'debug', 'normal'"""
        return self._mode

    @mode.setter
    def mode(self, value):
        self.debug("Logger mode set to: {}".format(value))

        val = value.lower()

        if val == "debug":
            self._mode = "debug"
        else:
            self._mode = "normal"

    def debug(self, message):
        """Prints if in debug mode."""
        if self._mode == "debug":
            self._handle_message("DEBUG", message)

    def error(self, message):
        """Always shows."""
        self._handle_message("ERROR", message)

    def _handle_message(self, level, message):
            message = "{}\t{}\t{}\n".format(str(datetime.utcnow()), level, message)
            if self.path_to_serialize is not None:
                self._write_to_disk(message)
            else:
                print(message)

    def _write_to_disk(self, message):
        with open(self.path_to_serialize, "a+") as f:
            f.write(message)
