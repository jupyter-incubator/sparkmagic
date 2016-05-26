# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import logging
import logging.config


class Log(object):
    """Logger for magics. A small wrapper class around the configured logger described in the configuration file"""
    def __init__(self, logger_name, logging_config, caller_name):
        logging.config.dictConfig(logging_config)
        
        assert caller_name is not None
        self._caller_name = caller_name
        self.logger_name = logger_name
        self._getLogger()

    def debug(self, message):
        self.logger.debug(self._transform_log_message(message))

    def error(self, message):
        self.logger.error(self._transform_log_message(message))

    def info(self, message):
        self.logger.info(self._transform_log_message(message))

    def _getLogger(self):
        self.logger = logging.getLogger(self.logger_name)

    def _transform_log_message(self, message):
        return u'{}\t{}'.format(self._caller_name, message)
    
