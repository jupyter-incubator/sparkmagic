# -*- coding: utf-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from six import string_types

from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME, MIMETYPE_TEXT_HTML, MIMETYPE_TEXT_PLAIN
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand
from sparkmagic.livyclientlib.exceptions import SparkStatementException
from sparkmagic.livyclientlib.sendpandasdftosparkcommand import SendPandasDfToSparkCommand
from sparkmagic.livyclientlib.sendstringtosparkcommand import SendStringToSparkCommand
from sparkmagic.livyclientlib.exceptions import BadUserDataException


@magics_class
class SparkMagicBase(Magics):

    _STRING_VAR_TYPE = 'str'
    _PANDAS_DATAFRAME_VAR_TYPE = 'df'
    _ALLOWED_LOCAL_TO_SPARK_TYPES = [_STRING_VAR_TYPE, _PANDAS_DATAFRAME_VAR_TYPE]

    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = SparkLog(u"SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        self.logger.debug(u'Initialized spark magics.')

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def do_send_to_spark(self, cell, input_variable_name, var_type, output_variable_name, max_rows, session_name):
        try:
            input_variable_value = self.shell.user_ns[input_variable_name]
        except KeyError:
            raise BadUserDataException(u'Variable named {} not found.'.format(input_variable_name))
        if input_variable_value is None:
            raise BadUserDataException(u'Value of {} is None!'.format(input_variable_name))

        if not output_variable_name:
            output_variable_name = input_variable_name

        if not max_rows:
            max_rows = conf.default_maxrows()

        input_variable_type = var_type.lower()
        if input_variable_type == self._STRING_VAR_TYPE:
            command = SendStringToSparkCommand(input_variable_name, input_variable_value, output_variable_name)
        elif input_variable_type == self._PANDAS_DATAFRAME_VAR_TYPE:
            command = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, max_rows)
        else:
            raise BadUserDataException(u'Invalid or incorrect -t type. Available are: [{}]'.format(u','.join(self._ALLOWED_LOCAL_TO_SPARK_TYPES)))

        (success, result, mime_type) = self.spark_controller.run_command(command, None)
        if not success:
            self.ipython_display.send_error(result)
        else:
            self.ipython_display.write(u'Successfully passed \'{}\' as \'{}\' to Spark'
                                       u' kernel'.format(input_variable_name, output_variable_name))

    def execute_spark(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):
        (success, out, mimetype) = self.spark_controller.run_command(Command(cell), session_name)
        if not success:
            if conf.shutdown_session_on_spark_statement_errors():
                self.spark_controller.cleanup()

            raise SparkStatementException(out)
        else:
            if isinstance(out, string_types):
                if mimetype == MIMETYPE_TEXT_HTML:
                    self.ipython_display.html(out)
                else:
                    self.ipython_display.write(out)
            else:
                self.ipython_display.display(out)
            if output_var is not None:
                spark_store_command = self._spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce)
                df = self.spark_controller.run_command(spark_store_command, session_name)
                self.shell.user_ns[output_var] = df

    @staticmethod
    def _spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce):
        return SparkStoreCommand(output_var, samplemethod, maxrows, samplefraction, coerce=coerce)

    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         session, output_var, quiet, coerce):
        sqlquery = self._sqlquery(cell, samplemethod, maxrows, samplefraction, coerce)
        df = self.spark_controller.run_sqlquery(sqlquery, session)
        if output_var is not None:
            self.shell.user_ns[output_var] = df
        if quiet:
            return None
        else:
            return df

    @staticmethod
    def _sqlquery(cell, samplemethod, maxrows, samplefraction, coerce):
        return SQLQuery(cell, samplemethod, maxrows, samplefraction, coerce=coerce)

    def _print_endpoint_info(self, info_sessions, current_session_id):
        if info_sessions:
            info_sessions = sorted(info_sessions, key=lambda s: s.id)
            html = get_sessions_info_html(info_sessions, current_session_id)
            self.ipython_display.html(html)
        else:
            self.ipython_display.html(u'No active sessions.')
