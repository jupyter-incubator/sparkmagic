# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from datetime import datetime
from os import path


class Log(object):
    """Logger."""

    def __init__(self):
        self._mode = "normal"
        self._path_to_logs = "~/sparkmagic/log/log.txt"
                
    @property
    def mode(self):
        """One of: 'debug', 'normal', 'error'"""
        return self._mode

    @mode.setter
    def mode(self, value):
        self.debug("Logger mode set to: {}".format(value))

        val = value.lower()

        if val == "debug":
            self._mode = "debug"
        elif val == "error":
            self._mode = "error"
        else:
            self._mode = "normal"

    def debug(self, message):
        """Prints if in debug mode."""
        if self._mode == "debug":
            self._handle_message("DEBUG", message)

    def error(self, message):
        """Prints if in debug mode."""
        if self.mode == "debug" or self.mode == "error":
            self._handle_message("ERROR", message)

    def _handle_message(self, level, message):
            message = "{}\t{}\t{}\n".format(str(datetime.utcnow()), level, message)
            # print(message)
            self._write_to_disk(message)

    def _write_to_disk(self, message):
        with open(path.expanduser(self._path_to_logs), "a+") as f:
            f.write(message)
