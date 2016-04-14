"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import json

from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope, cell_magic
from IPython.core.magic_arguments import argument, magic_arguments

import remotespark.utils.configuration as conf
from remotespark.livyclientlib.command import Command
from remotespark.livyclientlib.endpoint import Endpoint
from remotespark.magics.sparkmagicsbase import SparkMagicBase
from remotespark.utils.constants import LANGS_SUPPORTED
from remotespark.utils.sparkevents import SparkEvents
from remotespark.utils.utils import generate_uuid, get_livy_kind, parse_argstring_or_throw
from remotespark.livyclientlib.exceptions import handle_expected_exceptions, wrap_unexpected_exceptions, \
    BadUserDataException


def _event(f):
    def wrapped(self, *args, **kwargs):
        guid = self._generate_uuid()
        self._spark_events.emit_magic_execution_start_event(f.__name__, get_livy_kind(self.language), guid)
        try:
            result = f(self, *args, **kwargs)
        except Exception as e:
            self._spark_events.emit_magic_execution_end_event(f.__name__, get_livy_kind(self.language), guid,
                                                              False, e.__class__.__name__, str(e))
            raise
        else:
            self._spark_events.emit_magic_execution_end_event(f.__name__, get_livy_kind(self.language), guid,
                                                              True, "", "")
            return result
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped


@magics_class
class KernelMagics(SparkMagicBase):
    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(KernelMagics, self).__init__(shell, data)

        self.session_name = "session_name"
        self.session_started = False

        # In order to set these following 3 properties, call %%_do_not_call_change_language -l language
        self.language = ""
        self.endpoint = None
        self.fatal_error = False
        self.fatal_error_message = ""
        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events

    @magic_arguments()
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def help(self, line, cell="", local_ns=None):
        parse_argstring_or_throw(self.help, line)
        self._assure_cell_body_is_empty(KernelMagics.help.__name__, cell)
        help_html = """
<table>
  <tr>
    <th>Magic</th>
    <th>Example</th>
    <th>Explanation</th>
  </tr>
  <tr>
    <td>info</td>
    <td>%%info</td>
    <td>Outputs session information for the current Livy endpoint.</td>
  </tr>
  <tr>
    <td>cleanup</td>
    <td>%%cleanup -f</td>
    <td>Deletes all sessions for the current Livy endpoint, including this notebook's session. The force flag is mandatory.</td>
  </tr>
  <tr>
    <td>delete</td>
    <td>%%delete -f -s 0</td>
    <td>Deletes a session by number for the current Livy endpoint. Cannot delete this kernel's session.</td>
  </tr>
  <tr>
    <td>logs</td>
    <td>%%logs</td>
    <td>Outputs the current session's Livy logs.</td>
  </tr>
  <tr>
    <td>configure</td>
    <td>%%configure -f<br/>{"executorMemory": "1000M", "executorCores": 4}</td>
    <td>Configure the session creation parameters. The force flag is mandatory if a session has already been
    created and the session will be dropped and recreated.<br/>Look at <a href="https://github.com/cloudera/livy#request-body">
    Livy's POST /sessions Request Body</a> for a list of valid parameters. Parameters must be passed in as a JSON string.</td>
  </tr>
  <tr>
    <td>sql</td>
    <td>%%sql -o tables -q<br/>SHOW TABLES</td>
    <td>Executes a SQL query against the sqlContext.
    Parameters:
      <ul>
        <li>-o VAR_NAME: The result of the query will be available in the %%local Python context as a
          <a href="http://pandas.pydata.org/">Pandas</a> dataframe.</li>
        <li>-q: The magic will return None instead of the dataframe (no visualization).</li>
        <li>-m METHOD: Sample method, either <tt>take</tt> or <tt>sample</tt>.</li>
        <li>-n MAXROWS: The maximum number of rows of a SQL query that will be pulled from Livy to Jupyter.
            If this number is negative, then the number of rows will be unlimited.</li>
        <li>-r FRACTION: Fraction used for sampling.</li>
      </ul>
    </td>
  </tr>
  <tr>
    <td>local</td>
    <td>%%local<br/>a = 1</td>
    <td>All the code in subsequent lines will be executed locally. Code must be valid Python code.</td>
  </tr>
</table>
"""
        self.ipython_display.html(help_html)

    @cell_magic
    def local(self, line, cell="", local_ns=None):
        # This should not be reachable thanks to UserCodeParser. Registering it here so that it auto-completes with tab.
        raise NotImplementedError("UserCodeParser should have prevented code execution from reaching here.")

    @magic_arguments()
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def info(self, line, cell="", local_ns=None):
        parse_argstring_or_throw(self.info, line)
        self._assure_cell_body_is_empty(KernelMagics.info.__name__, cell)
        self.ipython_display.writeln("Endpoint:\n\t{}\n".format(self.endpoint.url))

        self.ipython_display.writeln("Current session ID number:\n\t{}\n".format(
                self.spark_controller.get_session_id_for_client(self.session_name)))

        self.ipython_display.writeln("Session configs:\n\t{}\n".format(conf.get_session_properties(self.language)))

        info_sessions = self.spark_controller.get_all_sessions_endpoint_info(self.endpoint)
        self.print_endpoint_info(info_sessions)

    @magic_arguments()
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def logs(self, line, cell="", local_ns=None):
        parse_argstring_or_throw(self.logs, line)
        self._assure_cell_body_is_empty(KernelMagics.logs.__name__, cell)
        if self.session_started:
            out = self.spark_controller.get_logs()
            self.ipython_display.write(out)
        else:
            self.ipython_display.write("No logs yet.")

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def configure(self, line, cell="", local_ns=None):
        try:
            dictionary = json.loads(cell)
        except ValueError:
            self.ipython_display.send_error("Could not parse JSON object from input '{}'".format(cell))
            return
        args = parse_argstring_or_throw(self.configure, line)
        if self.session_started:
            if not args.force:
                self.ipython_display.send_error("A session has already been started. If you intend to recreate the "
                                                "session with new configurations, please include the -f argument.")
                return
            else:
                self._do_not_call_delete_session("")
                self._override_session_settings(dictionary)
                self._do_not_call_start_session("")
        else:
            self._override_session_settings(dictionary)
        self.info("")

    @magic_arguments()
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def spark(self, line, cell="", local_ns=None):
        parse_argstring_or_throw(self.spark, line)
        if self._do_not_call_start_session(""):
            (success, out) = self.spark_controller.run_command(Command(cell))
            if success:
                self.ipython_display.write(out)
            else:
                self.ipython_display.send_error(out)
        else:
            return None

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, query will be stored in variable of this "
                                                             "name.")
    @argument("-q", "--quiet", type=bool, default=False, const=True, nargs="?", help="Return None instead of the dataframe.")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for SQL queries: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the server for SQL queries")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from SQL queries")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def sql(self, line, cell="", local_ns=None):
        if self._do_not_call_start_session(""):
            args = parse_argstring_or_throw(self.sql, line)
            return self.execute_sqlquery(cell, args.samplemethod, args.maxrows, args.samplefraction,
                                         None, args.output, args.quiet)
        else:
            return

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def cleanup(self, line, cell="", local_ns=None):
        self._assure_cell_body_is_empty(KernelMagics.cleanup.__name__, cell)
        args = parse_argstring_or_throw(self.cleanup, line)
        if args.force:
            self._do_not_call_delete_session("")

            self.spark_controller.cleanup_endpoint(self.endpoint)
        else:
            self.ipython_display.send_error("When you clean up the endpoint, all sessions will be lost, including the "
                                            "one used for this notebook. Include the -f parameter if that's your "
                                            "intention.")
            return

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @argument("-s", "--session", type=int, help="Session id number to delete.")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def delete(self, line, cell="", local_ns=None):
        self._assure_cell_body_is_empty(KernelMagics.delete.__name__, cell)
        args = parse_argstring_or_throw(self.delete, line)
        session = args.session

        if args.session is None:
            self.ipython_display.send_error('You must provide a session ID (-s argument).')
            return

        if args.force:
            id = self.spark_controller.get_session_id_for_client(self.session_name)
            if session == id:
                self.ipython_display.send_error("Cannot delete this kernel's session ({}). Specify a different session,"
                                                " shutdown the kernel to delete this session, or run %cleanup to "
                                                "delete all sessions for this endpoint.".format(id))
                return

            self.spark_controller.delete_session_by_id(self.endpoint, session)
        else:
            self.ipython_display.send_error("Include the -f parameter if you understand that all statements executed "
                                            "in this session will be lost.")

    @cell_magic
    def _do_not_call_start_session(self, line, cell="", local_ns=None):
        # Starts a session unless session is already created or a fatal error occurred. Returns True when session is
        # created successfully.
        # No need to add the handle_expected_exceptions decorator to this since we manually catch all
        # exceptions when starting the session.

        if self.fatal_error:
            self.ipython_display.send_error(self.fatal_error_message)
            return False

        if not self.session_started:
            skip = False
            properties = conf.get_session_properties(self.language)
            self.session_started = True

            try:
                self.spark_controller.add_session(self.session_name, self.endpoint, skip, properties)
            except Exception as e:
                self.fatal_error = True
                self.fatal_error_message = conf.fatal_error_suggestion().format(e)
                self.logger.error("Error creating session: {}".format(e))
                self.ipython_display.send_error(self.fatal_error_message)
                return False

        return self.session_started

    @cell_magic
    @handle_expected_exceptions
    def _do_not_call_delete_session(self, line, cell="", local_ns=None):
        try:
            if self.session_started:
                self.spark_controller.delete_session_by_name(self.session_name)
        except:
            # The exception will be logged and handled in the frontend.
            raise
        finally:
            self.session_started = False

    @magic_arguments()
    @cell_magic
    @argument("-l", "--language", type=str, help="Language to use.")
    def _do_not_call_change_language(self, line, cell="", local_ns=None):
        args = parse_argstring_or_throw(self._do_not_call_change_language, line)
        language = args.language.lower()

        if language not in LANGS_SUPPORTED:
            self.ipython_display.send_error("'{}' language not supported in kernel magics.".format(language))
            return

        if self.session_started:
            self.ipython_display.send_error("Cannot change the language if a session has been started.")
            return

        self.language = language
        self.refresh_configuration()

    def refresh_configuration(self):
        credentials = getattr(conf, 'kernel_' + self.language + '_credentials')()
        (username, password, url) = (credentials['username'], credentials['password'], credentials['url'])
        self.endpoint = Endpoint(url, username, password)

    def get_session_settings(self, line, force):
        line = line.strip()
        if not force:
            return line
        else:
            if line.startswith("-f "):
                return line[3:]
            elif line.endswith(" -f"):
                return line[:-3]
            else:
                return None

    @staticmethod
    def _override_session_settings(settings):
        conf.override(conf.session_configs.__name__, settings)

    @staticmethod
    def _generate_uuid():
        return generate_uuid()

    @staticmethod
    def _assure_cell_body_is_empty(magic_name, cell):
        if cell.strip():
            raise BadUserDataException("Cell body for %%{} magic must be empty; got '{}' instead"
                                       .format(magic_name, cell.strip()))


def load_ipython_extension(ip):
    ip.register_magics(KernelMagics)
