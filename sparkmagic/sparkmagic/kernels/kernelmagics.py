"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
import json
from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope, cell_magic, line_magic
from IPython.core.magic_arguments import argument, magic_arguments
from hdijupyterutils.utils import generate_uuid
import importlib

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.configuration import get_livy_kind
from sparkmagic.utils import constants
from sparkmagic.utils.utils import parse_argstring_or_throw, get_coerce_value, initialize_auth, Namespace
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.constants import LANGS_SUPPORTED
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.livyclientlib.exceptions import handle_expected_exceptions, wrap_unexpected_exceptions, \
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
                                                              True, u"", u"")
            return result
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped


@magics_class
class KernelMagics(SparkMagicBase):
    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(KernelMagics, self).__init__(shell, data)

        self.session_name = u"session_name"
        self.session_started = False

        # In order to set these following 3 properties, call %%_do_not_call_change_language -l language
        self.language = u""
        self.endpoint = None
        self.fatal_error = False
        self.fatal_error_message = u""
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
        help_html = u"""
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
    <td>spark</td>
    <td>%%spark -o df<br/>df = spark.read.parquet('...</td>
    <td>Executes spark commands.
    Parameters:
      <ul>
        <li>-o VAR_NAME: The Spark dataframe of name VAR_NAME will be available in the %%local Python context as a
          <a href="http://pandas.pydata.org/">Pandas</a> dataframe with the same name.</li>
        <li>-m METHOD: Sample method, either <tt>take</tt> or <tt>sample</tt>.</li>
        <li>-n MAXROWS: The maximum number of rows of a dataframe that will be pulled from Livy to Jupyter.
            If this number is negative, then the number of rows will be unlimited.</li>
        <li>-r FRACTION: Fraction used for sampling.</li>
      </ul>
    </td>
  </tr>
  <tr>
    <td>sql</td>
    <td>%%sql -o tables -q<br/>SHOW TABLES</td>
    <td>Executes a SQL query against the variable sqlContext (Spark v1.x) or spark (Spark v2.x).
    Parameters:
      <ul>
        <li>-o VAR_NAME: The result of the SQL query will be available in the %%local Python context as a
          <a href="http://pandas.pydata.org/">Pandas</a> dataframe.</li>
        <li>-q: The magic will return None instead of the dataframe (no visualization).</li>
        <li>-m, -n, -r are the same as the %%spark parameters above.</li>
      </ul>
    </td>
  </tr>
  <tr>
    <td>local</td>
    <td>%%local<br/>a = 1</td>
    <td>All the code in subsequent lines will be executed locally. Code must be valid Python code.</td>
  </tr>
  <tr>
    <td>send_to_spark</td>
    <td>%%send_to_spark -i variable -t str -n var</td>
    <td>Sends a variable from local output to spark cluster.
    <br/>
    Parameters:
      <ul>
        <li>-i VAR_NAME: Local Pandas DataFrame(or String) of name VAR_NAME will be available in the %%spark context as a 
          Spark dataframe(or String) with the same name.</li>
        <li>-t TYPE: Specifies the type of variable passed as -i. Available options are:
         `str` for string and `df` for Pandas DataFrame. Optional, defaults to `str`.</li>
        <li>-n NAME: Custom name of variable passed as -i. Optional, defaults to -i variable name.</li>
        <li>-m MAXROWS: Maximum amount of Pandas rows that will be sent to Spark. Defaults to 2500.</li>
      </ul>
    </td>
  </tr>
</table>
"""
        self.ipython_display.html(help_html)

    @cell_magic
    def local(self, line, cell=u"", local_ns=None):
        # This should not be reachable thanks to UserCodeParser. Registering it here so that it auto-completes with tab.
        raise NotImplementedError(u"UserCodeParser should have prevented code execution from reaching here.")

    @magic_arguments()
    @argument("-i", "--input", type=str, default=None, help="If present, indicated variable will be stored in variable"
                                                             " in Spark's context.")
    @argument("-t", "--vartype", type=str, default='str', help="Optionally specify the type of input variable. "
                                                               "Available: 'str' - string(default) or 'df' - Pandas DataFrame")
    @argument("-n", "--varname", type=str, default=None, help="Optionally specify the custom name for the input variable.")
    @argument("-m", "--maxrows", type=int, default=2500, help="Maximum number of rows that will be pulled back "
                                                              "from the local dataframe")
    @cell_magic
    @needs_local_scope
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def send_to_spark(self, line, cell=u"", local_ns=None):
        self._assure_cell_body_is_empty(KernelMagics.send_to_spark.__name__, cell)
        args = parse_argstring_or_throw(self.send_to_spark, line)

        if not args.input:
            raise BadUserDataException("-i param not provided.")

        if self._do_not_call_start_session(""):
            self.do_send_to_spark(cell, args.input, args.vartype, args.varname, args.maxrows, None)
        else:
            return

    @magic_arguments()
    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    @_event
    def info(self, line, cell=u"", local_ns=None):
        parse_argstring_or_throw(self.info, line)
        self._assure_cell_body_is_empty(KernelMagics.info.__name__, cell)
        if self.session_started:
            current_session_id = self.spark_controller.get_session_id_for_client(self.session_name)
        else:
            current_session_id = None

        self.ipython_display.html(u"Current session configs: <tt>{}</tt><br>".format(conf.get_session_properties(self.language)))

        info_sessions = self.spark_controller.get_all_sessions_endpoint(self.endpoint)
        self._print_endpoint_info(info_sessions, current_session_id)

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
            self.ipython_display.write(u"No logs yet.")

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
            self.ipython_display.send_error(u"Could not parse JSON object from input '{}'".format(cell))
            return
        args = parse_argstring_or_throw(self.configure, line)
        if self.session_started:
            if not args.force:
                self.ipython_display.send_error(u"A session has already been started. If you intend to recreate the "
                                                u"session with new configurations, please include the -f argument.")
                return
            else:
                self._do_not_call_delete_session(u"")
                self._override_session_settings(dictionary)
                self._do_not_call_start_session(u"")
        else:
            self._override_session_settings(dictionary)
        self.info(u"")

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, indicated variable will be stored in variable"
                                                             "of this name in user's local context.")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for dataframe: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the dataframe on the server for storing")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from dataframe")
    @argument("-c", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) " 
                                                                        "of the dataframe or not (pass False)")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def spark(self, line, cell="", local_ns=None):
        if self._do_not_call_start_session(u""):
            args = parse_argstring_or_throw(self.spark, line)

            coerce = get_coerce_value(args.coerce)

            self.execute_spark(cell, args.output, args.samplemethod, args.maxrows, args.samplefraction, None, coerce)
        else:
            return

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
    @argument("-c", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) " 
                                                                        "of the dataframe or not (pass False)")
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def sql(self, line, cell="", local_ns=None):
        if self._do_not_call_start_session(""):
            args = parse_argstring_or_throw(self.sql, line)

            coerce = get_coerce_value(args.coerce)

            return self.execute_sqlquery(cell, args.samplemethod, args.maxrows, args.samplefraction,
                                         None, args.output, args.quiet, coerce)
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
            self._do_not_call_delete_session(u"")

            self.spark_controller.cleanup_endpoint(self.endpoint)
        else:
            self.ipython_display.send_error(u"When you clean up the endpoint, all sessions will be lost, including the "
                                            u"one used for this notebook. Include the -f parameter if that's your "
                                            u"intention.")
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
            self.ipython_display.send_error(u'You must provide a session ID (-s argument).')
            return

        if args.force:
            id = self.spark_controller.get_session_id_for_client(self.session_name)
            if session == id:
                self.ipython_display.send_error(u"Cannot delete this kernel's session ({}). Specify a different session,"
                                                u" shutdown the kernel to delete this session, or run %cleanup to "
                                                u"delete all sessions for this endpoint.".format(id))
                return

            self.spark_controller.delete_session_by_id(self.endpoint, session)
        else:
            self.ipython_display.send_error(u"Include the -f parameter if you understand that all statements executed "
                                            u"in this session will be lost.")

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
                self.logger.error(u"Error creating session: {}".format(e))
                self.ipython_display.send_error(self.fatal_error_message)

                if conf.all_errors_are_fatal():
                    raise e

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
            self.ipython_display.send_error(u"'{}' language not supported in kernel magics.".format(language))
            return

        if self.session_started:
            self.ipython_display.send_error(u"Cannot change the language if a session has been started.")
            return

        self.language = language
        self.refresh_configuration()

    @magic_arguments()
    @line_magic
    @argument("-u", "--username", dest='user', type=str, help="Username to use.")
    @argument("-p", "--password", type=str, help="Password to use.")
    @argument("-s", "--server", dest='url', type=str, help="Url of server to use.")
    @argument("-t", "--auth", type=str, help="Auth type for authentication")
    @_event
    def _do_not_call_change_endpoint(self, line, cell="", local_ns=None):
        args = parse_argstring_or_throw(self._do_not_call_change_endpoint, line)
        if self.session_started:
            error = u"Cannot change the endpoint if a session has been started."
            raise BadUserDataException(error)
        auth = initialize_auth(args=args)
        self.endpoint = Endpoint(args.url, auth)

    @line_magic
    def matplot(self, line, cell="", local_ns=None):
        session = self.spark_controller.get_session_by_name_or_default(self.session_name)
        command = Command("%matplot " + line)
        (success, out, mimetype) = command.execute(session)
        if success:
            session.ipython_display.display(out)
        else:
            session.ipython_display.send_error(out)

    def refresh_configuration(self):
        credentials = getattr(conf, 'base64_kernel_' + self.language + '_credentials')()
        (username, password, auth, url) = (credentials['username'], credentials['password'], credentials['auth'], credentials['url'])
        args = Namespace(auth=auth, user=username, password=password, url=url)
        auth_instance = initialize_auth(args)
        self.endpoint = Endpoint(url, auth_instance)

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
            raise BadUserDataException(u"Cell body for %%{} magic must be empty; got '{}' instead"
                                       .format(magic_name, cell.strip()))

def load_ipython_extension(ip):
    ip.register_magics(KernelMagics)
