# -*- coding: utf-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in
remote cluster.

Provides the %spark magic.
"""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from six import string_types
import json

from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay
from collections import namedtuple
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html
from sparkmagic.utils.constants import MIMETYPE_TEXT_HTML
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand
from sparkmagic.livyclientlib.exceptions import SparkStatementException
from sparkmagic.livyclientlib.sendpandasdftosparkcommand import (
    SendPandasDfToSparkCommand,
)
from sparkmagic.livyclientlib.sendstringtosparkcommand import SendStringToSparkCommand
from sparkmagic.livyclientlib.exceptions import BadUserDataException

# How to display different cell content types in IPython
SparkOutputHandler = namedtuple("SparkOutputHandler", ["html", "text", "default"])


def looks_like_json(s):
    return s.startswith("{") and s.endswith("}")


@magics_class
class SparkMagicBase(Magics):
    _STRING_VAR_TYPE = "str"
    _PANDAS_DATAFRAME_VAR_TYPE = "df"
    _ALLOWED_LOCAL_TO_SPARK_TYPES = [_STRING_VAR_TYPE, _PANDAS_DATAFRAME_VAR_TYPE]

    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = SparkLog("SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        self.logger.debug("Initialized spark magics.")

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def do_send_to_spark(
        self,
        cell,
        input_variable_name,
        var_type,
        output_variable_name,
        max_rows,
        session_name,
    ):
        try:
            input_variable_value = self.shell.user_ns[input_variable_name]
        except KeyError:
            raise BadUserDataException(
                "Variable named {} not found.".format(input_variable_name)
            )
        if input_variable_value is None:
            raise BadUserDataException(
                "Value of {} is None!".format(input_variable_name)
            )

        if not output_variable_name:
            output_variable_name = input_variable_name

        if not max_rows:
            max_rows = conf.default_maxrows()

        input_variable_type = var_type.lower()
        if input_variable_type == self._STRING_VAR_TYPE:
            command = SendStringToSparkCommand(
                input_variable_name, input_variable_value, output_variable_name
            )
        elif input_variable_type == self._PANDAS_DATAFRAME_VAR_TYPE:
            command = SendPandasDfToSparkCommand(
                input_variable_name,
                input_variable_value,
                output_variable_name,
                max_rows,
            )
        else:
            raise BadUserDataException(
                "Invalid or incorrect -t type. Available are: [{}]".format(
                    ",".join(self._ALLOWED_LOCAL_TO_SPARK_TYPES)
                )
            )

        (success, result, mime_type) = self.spark_controller.run_command(command, None)
        if not success:
            self.ipython_display.send_error(result)
        else:
            self.ipython_display.write(
                "Successfully passed '{}' as '{}' to Spark"
                " kernel".format(input_variable_name, output_variable_name)
            )

    def execute_spark(
        self,
        cell,
        output_var,
        samplemethod,
        maxrows,
        samplefraction,
        session_name,
        coerce,
        output_handler=None,
    ):
        output_handler = output_handler or SparkOutputHandler(
            html=self.ipython_display.html,
            text=self.ipython_display.write,
            default=self.ipython_display.display,
        )

        (success, out, mimetype) = self.spark_controller.run_command(
            Command(cell), session_name
        )
        if not success:
            if conf.shutdown_session_on_spark_statement_errors():
                self.spark_controller.cleanup()

            raise SparkStatementException(out)
        else:
            if isinstance(out, string_types):
                if mimetype == MIMETYPE_TEXT_HTML:
                    output_handler.html(out)
                else:
                    # Check for special case of { "text/html": "<div>...</div>" }
                    # which is return by Livy from IPython display or display_html
                    # parse out the html and display it
                    if looks_like_json(out):
                        try:
                            # output will be in dict format (single quotes) so convert to JSON double quotes
                            out_dict = json.loads(out.replace("'", '"'))
                            if MIMETYPE_TEXT_HTML in out_dict:
                                # display the html
                                output_handler.html(out_dict[MIMETYPE_TEXT_HTML])
                            else:
                                # treat as text
                                output_handler.text(out)
                        except ValueError:
                            # treat as text
                            output_handler.text(out)
                    else:
                        output_handler.text(out)
            else:
                output_handler.default(out)
            if output_var is not None:
                spark_store_command = self._spark_store_command(
                    output_var, samplemethod, maxrows, samplefraction, coerce
                )
                df = self.spark_controller.run_command(
                    spark_store_command, session_name
                )
                self.shell.user_ns[output_var] = df

    @staticmethod
    def _spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce):
        return SparkStoreCommand(
            output_var, samplemethod, maxrows, samplefraction, coerce=coerce
        )

    def execute_sqlquery(
        self,
        cell,
        samplemethod,
        maxrows,
        samplefraction,
        session,
        output_var,
        quiet,
        coerce,
    ):
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
            self.ipython_display.html("No active sessions.")
