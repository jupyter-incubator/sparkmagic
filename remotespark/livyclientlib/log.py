# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from datetime import datetime

from .utils import join_paths, get_magics_home_path, get_instance_id, ensure_path_exists, ensure_file_exists
from .configuration import get_configuration
from .constants import Constants


class Log(object):
    """Logger."""

    def __init__(self, caller_name):
        assert caller_name is not None

        self._caller_name = caller_name

        self._level = Constants.normal_level
        self._path_to_serialize = None

        self._read_config()

    @property
    def level(self):
        """One of: 'debug', 'normal'"""
        return self._level

    def _read_config(self):
        # Create path name
        magics_home_path = get_magics_home_path()
        logs_folder_name = "logs"
        log_file_name = "log_{}.log".format(get_instance_id())
        self._path_to_serialize = join_paths(magics_home_path, logs_folder_name)
        ensure_path_exists(self._path_to_serialize)
        self._path_to_serialize = join_paths(self._path_to_serialize, log_file_name)
        ensure_file_exists(self._path_to_serialize)

        # Read log level
        level = get_configuration(Constants.log_level, Constants.debug_level)

        self._level = level

    def debug(self, message):
        """Prints if in debug mode."""
        self._handle_message("DEBUG", message, [Constants.debug_level])

    def error(self, message):
        """Always shows."""
        self._handle_message("ERROR", message, [Constants.normal_level, Constants.debug_level])

    def _handle_message(self, level, message, accepted_modes):
        if self.level in accepted_modes:
            message = "{}\t{}\t{}\t{}\n".format(str(datetime.utcnow()), level, self._caller_name, message)

            if get_configuration(Constants.log_to_disk, True):
                self._write_to_disk(message)
            else:
                print(message)

    def _write_to_disk(self, message):
        with open(self._path_to_serialize, "a+") as f:
            f.write(message)
