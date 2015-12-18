# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import logging
import logging.config

import remotespark.utils.configuration as conf
from remotespark.utils.constants import Constants


class Log(object):
    """Logger for magics. A small wrapper class around the configured logger described in the configuration file"""
    logging.config.dictConfig(conf.logging_config())

    def __init__(self, caller_name):
        assert caller_name is not None
        self._caller_name = caller_name
        self._getLogger()
    
    def debug(self, message):
        self.logger.debug(self._transform_log_message(message))

    def error(self, message):
        self.logger.error(self._transform_log_message(message))

    def _getLogger(self):
        self.logger = logging.getLogger(Constants.magics_logger_name)

    def _transform_log_message(self, message):
        return '{}\t{}'.format(self._caller_name, message)
    
