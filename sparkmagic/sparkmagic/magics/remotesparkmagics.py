"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
import json
from IPython.core.magic import line_cell_magic, needs_local_scope, line_magic
from IPython.core.magic import magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.utils import parse_argstring_or_throw, get_coerce_value, initialize_auth
from sparkmagic.utils.constants import CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, LANG_PYTHON, LANG_R, LANG_SCALA
from sparkmagic.controllerwidget.magicscontrollerwidget import MagicsControllerWidget
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.livyclientlib.exceptions import handle_expected_exceptions


@magics_class
class RemoteSparkMagics(SparkMagicBase):
    def __init__(self, shell, data=None, widget=None):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell, data)

        self.endpoints = {}
        if widget is None:
            widget = MagicsControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display)
        self.manage_widget = widget

    @line_magic
    def manage_spark(self, line, local_ns=None):
        """Magic to manage Spark endpoints and sessions. First, add an endpoint via the 'Add Endpoint' tab.
        Then, create a session. You'll be able to select the session created from the %%spark magic."""
        return self.manage_widget

    @magic_arguments()
    @argument("-c", "--context", type=str, default=CONTEXT_NAME_SPARK,
              help="Context to use: '{}' for spark and '{}' for sql queries. "
                   "Default is '{}'.".format(CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, CONTEXT_NAME_SPARK))
    @argument("-s", "--session", type=str, default=None, help="The name of the Livy session to use.")
    @argument("-o", "--output", type=str, default=None, help="If present, output when using SQL "
                                                             "queries will be stored in this variable.")
    @argument("-q", "--quiet", type=bool, default=False, nargs="?", const=True, help="Do not display visualizations"
                                                                                     " on SQL queries")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for SQL queries: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the server for SQL queries")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from SQL queries")
    @argument("-u", "--url", type=str, default=None, help="URL for Livy endpoint")
    @argument("-a", "--user", dest='user', type=str, default="", help="Username for HTTP access to Livy endpoint")
    @argument("-p", "--password", type=str, default="", help="Password for HTTP access to Livy endpoint")
    @argument("-t", "--auth", type=str, default=None, help="Auth type for HTTP access to Livy endpoint. [Kerberos, None, Basic]")
    @argument("-l", "--language", type=str, default=None,
              help="Language for Livy session; one of {}".format(', '.join([LANG_PYTHON, LANG_SCALA, LANG_R])))
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @argument("-k", "--skip", type=bool, default=False, nargs="?", const=True, help="Skip adding session if it already exists")
    @argument("-i", "--id", type=int, default=None, help="Session ID")
    @argument("-e", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) "
                                                                        "of the dataframe or not (pass False)")

    @needs_local_scope
    @line_cell_magic
    @handle_expected_exceptions
    def spark(self, line, cell="", local_ns=None):
        """Magic to execute spark remotely.

           This magic allows you to create a Livy Scala or Python session against a Livy endpoint. Every session can
           be used to execute either Spark code or SparkSQL code by executing against the SQL context in the session.
           When the SQL context is used, the result will be a Pandas dataframe of a sample of the results.

           If invoked with no subcommand, the cell will be executed against the specified session.

           Subcommands
           -----------
           info
               Display the available Livy sessions and other configurations for sessions.
           add
               Add a Livy session given a session name (-s), language (-l), and endpoint credentials.
               The -k argument, if present, will skip adding this session if it already exists.
               e.g. `%spark add -s test -l python -u https://sparkcluster.net/livy -t Kerberos -a u -p -k`
           config
               Override the livy session properties sent to Livy on session creation. All session creations will
               contain these config settings from then on.
               Expected value is a JSON key-value string to be sent as part of the Request Body for the POST /sessions
               endpoint in Livy.
               e.g. `%%spark config`
                    `{"driverMemory":"1000M", "executorCores":4}`
           run
               Run Spark code against a session.
               e.g. `%%spark -s testsession` will execute the cell code against the testsession previously created
               e.g. `%%spark -s testsession -c sql` will execute the SQL code against the testsession previously created
               e.g. `%%spark -s testsession -c sql -o my_var` will execute the SQL code against the testsession
                        previously created and store the pandas dataframe created in the my_var variable in the
                        Python environment.
           logs
               Returns the logs for a given session.
               e.g. `%spark logs -s testsession` will return the logs for the testsession previously created
           delete
               Delete a Livy session.
               e.g. `%spark delete -s defaultlivy`
           cleanup
               Delete all Livy sessions created by the notebook. No arguments required.
               e.g. `%spark cleanup`
        """
        usage = "Please look at usage of %spark by executing `%spark?`."
        user_input = line
        args = parse_argstring_or_throw(self.spark, user_input)

        subcommand = args.command[0].lower()
  
        if args.auth is None:
            args.auth = conf.get_auth_value(args.user, args.password)
        else:
            args.auth = args.auth

        # info
        if subcommand == "info":
            if args.url is not None and args.id is not None:
                endpoint = Endpoint(args.url, initialize_auth(args))
                info_sessions = self.spark_controller.get_all_sessions_endpoint_info(endpoint)
                self._print_endpoint_info(info_sessions, args.id)
            else:
                self._print_local_info()
        # config
        elif subcommand == "config":
            conf.override(conf.session_configs.__name__, json.loads(cell))
        # add
        elif subcommand == "add":
            if args.url is None:
                self.ipython_display.send_error("Need to supply URL argument (e.g. -u https://example.com/livyendpoint)")
                return

            name = args.session
            language = args.language
            
            endpoint = Endpoint(args.url, initialize_auth(args))
            skip = args.skip

            properties = conf.get_session_properties(language)

            self.spark_controller.add_session(name, endpoint, skip, properties)
        # delete
        elif subcommand == "delete":
            if args.session is not None:
                self.spark_controller.delete_session_by_name(args.session)
            elif args.url is not None:
                if args.id is None:
                    self.ipython_display.send_error("Must provide --id or -i option to delete session at endpoint from URL")
                    return
                endpoint = Endpoint(args.url, initialize_auth(args))
                session_id = args.id
                self.spark_controller.delete_session_by_id(endpoint, session_id)
            else:
                self.ipython_display.send_error("Subcommand 'delete' requires a session name or a URL and session ID")
        # cleanup
        elif subcommand == "cleanup":
            if args.url is not None:
                endpoint = Endpoint(args.url, initialize_auth(args))
                self.spark_controller.cleanup_endpoint(endpoint)
            else:
                self.spark_controller.cleanup()
        # logs
        elif subcommand == "logs":
            self.ipython_display.write(self.spark_controller.get_logs(args.session))
        # run
        elif len(subcommand) == 0:
            coerce = get_coerce_value(args.coerce)
            if args.context == CONTEXT_NAME_SPARK:
                return self.execute_spark(cell, args.output, args.samplemethod,
                                          args.maxrows, args.samplefraction, args.session, coerce)
            elif args.context == CONTEXT_NAME_SQL:
                return self.execute_sqlquery(cell, args.samplemethod, args.maxrows, args.samplefraction,
                                             args.session, args.output, args.quiet, coerce)
            else:
                self.ipython_display.send_error("Context '{}' not found".format(args.context))
        # error
        else:
            self.ipython_display.send_error("Subcommand '{}' not found. {}".format(subcommand, usage))

    def _print_local_info(self):
        sessions_info = ["        {}".format(i) for i in self.spark_controller.get_manager_sessions_str()]
        print("""Info for running Spark:
    Sessions:
{}
    Session configs:
        {}
""".format("\n".join(sessions_info), conf.session_configs()))

def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
