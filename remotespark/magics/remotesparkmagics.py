"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import json

from IPython.core.magic import line_cell_magic, needs_local_scope
from IPython.core.magic import magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

import remotespark.utils.configuration as conf
from remotespark.controllerwidget.magicscontrollerwidget import MagicsControllerWidget
from remotespark.livyclientlib.command import Command
from remotespark.livyclientlib.endpoint import Endpoint
from remotespark.livyclientlib.sqlquery import SQLQuery
from remotespark.magics.sparkmagicsbase import SparkMagicBase
from remotespark.utils.constants import CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, LANG_PYTHON, LANG_R, LANG_SCALA
from remotespark.utils.ipywidgetfactory import IpyWidgetFactory


@magics_class
class RemoteSparkMagics(SparkMagicBase):
    def __init__(self, shell, data=None, widget=None):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell, data)

        self.endpoints = {}
        if widget is None:
            widget = MagicsControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display)
        self.manage_widget = widget

    @line_cell_magic
    def manage_spark(self, line, cell="", local_ns=None):
        return self.manage_widget

    @magic_arguments()
    @argument("-c", "--context", type=str, default=CONTEXT_NAME_SPARK,
              help="Context to use: '{}' for spark and '{}' for sql queries"
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
    @argument("-a", "--user", type=str, default="", help="Username for HTTP access to Livy endpoint")
    @argument("-p", "--password", type=str, default="", help="Password for HTTP access to Livy endpoint")
    @argument("-l", "--language", type=str, default=None,
              help="Language for Livy session; one of {}".format(', '.join([LANG_PYTHON, LANG_SCALA, LANG_R])))
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @argument("-k", "--skip", type=bool, default=False, nargs="?", const=True, help="Skip adding session if it already exists")
    @argument("-i", "--id", type=int, default=None, help="Session ID")
    @needs_local_scope
    @line_cell_magic
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
               e.g. `%%spark add -s test -l python -u https://sparkcluster.net/livy -a u -p -k`
           config
               Override the livy session properties sent to Livy on session creation. All session creations will
               contain these config settings from then on.
               Expected value is a JSON key-value string to be sent as part of the Request Body for the POST /sessions
               endpoint in Livy.
               e.g. `%%spark config {"driverMemory":"1000M", "executorCores":4}`
           run
               Run Spark code against a session.
               e.g. `%%spark -s testsession` will execute the cell code against the testsession previously created
               e.g. `%%spark -s testsession -c sql` will execute the SQL code against the testsession previously created
               e.g. `%%spark -s testsession -c sql -o my_var` will execute the SQL code against the testsession
                        previously created and store the pandas dataframe created in the my_var variable in the
                        Python environment.
           logs
               Returns the logs for a given session.
               e.g. `%%spark logs -s testsession` will return the logs for the testsession previously created
           delete
               Delete a Livy session.
               e.g. `%%spark delete -s defaultlivy`
           cleanup
               Delete all Livy sessions created by the notebook. No arguments required.
               e.g. `%%spark cleanup`
        """
        usage = "Please look at usage of %spark by executing `%spark?`."
        user_input = line
        args = parse_argstring(self.spark, user_input)

        subcommand = args.command[0].lower()

        try:
            # info
            if subcommand == "info":
                if args.url is not None:
                    endpoint = Endpoint(args.url, args.user, args.password)
                    info_sessions = self.spark_controller.get_all_sessions_endpoint_info(endpoint)
                    self.print_endpoint_info(info_sessions)
                else:
                    self._print_local_info()
            # config
            elif subcommand == "config":
                # Would normally do " ".join(args.command[1:]) but parse_argstring removes quotes...
                rest_of_line = user_input[7:]
                conf.override(conf.session_configs.__name__, json.loads(rest_of_line))
            # add
            elif subcommand == "add":
                if args.url is None:
                    raise ValueError("Need to supply URL argument (e.g. -u https://example.com/livyendpoint")

                name = args.session
                language = args.language
                endpoint = Endpoint(args.url, args.user, args.password)
                skip = args.skip

                properties = conf.get_session_properties(language)

                self.spark_controller.add_session(name, endpoint, skip, properties)
            # delete
            elif subcommand == "delete":
                if args.session is not None:
                    self.spark_controller.delete_session_by_name(args.session)
                elif args.url is not None:
                    if args.id is None:
                        raise ValueError("Must provide --id or -i option to delete session at endpoint from URL")
                    endpoint = Endpoint(args.url, args.user, args.password)
                    session_id = args.id
                    self.spark_controller.delete_session_by_id(endpoint, session_id)
                else:
                    raise ValueError("Subcommand 'delete' requires a session name or a URL and session ID")
            # cleanup
            elif subcommand == "cleanup":
                if args.url is not None:
                    endpoint = Endpoint(args.url, args.user, args.password)
                    self.spark_controller.cleanup_endpoint(endpoint)
                else:
                    self.spark_controller.cleanup()
            # logs
            elif subcommand == "logs":
                (success, out) = self.spark_controller.get_logs(args.session)
                if success:
                    self.ipython_display.write(out)
                else:
                    self.ipython_display.send_error(out)
            # run
            elif len(subcommand) == 0:
                if args.context == CONTEXT_NAME_SPARK:
                    (success, out) = self.spark_controller.run_command(Command(cell), args.session)
                    if success:
                        self.ipython_display.write(out)
                    else:
                        self.ipython_display.send_error(out)
                elif args.context == CONTEXT_NAME_SQL:
                    sqlquery = SQLQuery(cell, args.samplemethod, args.maxrows, args.samplefraction)
                    return self.execute_sqlquery(sqlquery, args.session, args.output,
                                                 args.quiet)
                else:
                    raise ValueError("Context '{}' not found".format(args.context))
            # error
            else:
                raise ValueError("Subcommand '{}' not found. {}".format(subcommand, usage))
        except ValueError as err:
            self.ipython_display.send_error("{}".format(err))

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
