# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from datetime import datetime

from .utils import join_paths, get_magics_home_path, read_environment_variable


class Log(object):
    """Logger."""

    def __init__(self, caller_name):
        assert caller_name is not None

        self._caller_name = caller_name

        self._mode = "normal"
        self.path_to_serialize = None

        self.read_config()

    @property
    def mode(self):
        """One of: 'debug', 'normal'"""
        return self._mode

    def read_config(self):
        # Read path
        try:
            magics_home_path = get_magics_home_path()
            self.path_to_serialize = join_paths(magics_home_path, "logs/log.txt")
        except KeyError:
            self.path_to_serialize = None

        # Read log level
        try:
            level = read_environment_variable("SPARKMAGIC_LOG_LEVEL").lower()

            if level != "debug":
                level = "normal"
        except KeyError:
            level = "normal"

        self._mode = level

    def debug(self, message):
        """Prints if in debug mode."""
        self._handle_message("DEBUG", message, ["debug"])

    def error(self, message):
        """Always shows."""
        self._handle_message("ERROR", message, ["normal", "debug"])

    def _handle_message(self, level, message, accepted_modes):
        self.read_config()

        if self.mode in accepted_modes:
            message = "{}\t{}\t{}\t{}\n".format(str(datetime.utcnow()), level, self._caller_name, message)

            if self.path_to_serialize is not None:
                self._write_to_disk(message)
            else:
                print(message)

    def _write_to_disk(self, message):
        with open(self.path_to_serialize, "a+") as f:
            f.write(message)
