﻿"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class, line_cell_magic, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
import json
import copy

import remotespark.utils.configuration as conf
from remotespark.utils.constants import Constants
from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.log import Log
from remotespark.utils.utils import get_magics_home_path, join_paths
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException
from remotespark.livyclientlib.sparkcontroller import SparkController


@magics_class
class RemoteSparkMagics(Magics):
    def __init__(self, shell, data=None):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)

        self.logger = Log("RemoteSparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        try:
            should_serialize = conf.serialize()
            if should_serialize:
                self.logger.debug("Serialization enabled.")

                self.magics_home_path = get_magics_home_path()
                path_to_serialize = join_paths(self.magics_home_path, "state.json")

                self.logger.debug("Will serialize to {}.".format(path_to_serialize))

                self.spark_controller = SparkController(self.ipython_display, serialize_path=path_to_serialize)
            else:
                self.logger.debug("Serialization NOT enabled.")
        except KeyError:
            self.logger.error("Could not read env vars for serialization.")

        self.logger.debug("Initialized spark magics.")

    @magic_arguments()
    @argument("-c", "--context", type=str, default=Constants.context_name_spark,
              help="Context to use: '{}' for spark, '{}' for sql queries, and '{}' for hive queries. "
                   "Default is '{}'.".format(Constants.context_name_spark,
                                             Constants.context_name_sql,
                                             Constants.context_name_hive,
                                             Constants.context_name_spark))
    @argument("-s", "--session", help="The name of the Livy session to use. "
                                      "If only one session has been created, there's no need to specify one.")
    @argument("-o", "--output", type=str, default=None, help="If present, output when using SQL or Hive "
                                                             "query will be stored in variable of this name.")
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
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
               Add a Livy session. First argument is the name of the session, second argument
               is the language, and third argument is the connection string of the Livy endpoint.
               A fourth argument specifying if session creation can be skipped if it already exists is optional:
               "skip" or empty.
               e.g. `%%spark add test python url=https://sparkcluster.net/livy;username=u;password=p skip`
               or
               e.g. `%%spark add test python url=https://sparkcluster.net/livy;username=u;password=p`
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
               Delete a Livy session. Argument is the name of the session to be deleted.
               e.g. `%%spark delete defaultlivy`
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
                if len(args.command) == 2:
                    connection_string = args.command[1]
                    info_sessions = self.spark_controller.get_all_sessions_endpoint_info(connection_string)
                    self._print_endpoint_info(info_sessions)
                elif len(args.command) == 1:
                    self._print_local_info()
                else:
                    raise ValueError("Subcommand 'info' requires no value or a connection string to show all sessions.\n"
                                     "{}".format(usage))
            # config
            elif subcommand == "config":
                # Would normally do " ".join(args.command[1:]) but parse_argstring removes quotes...
                rest_of_line = user_input[7:]
                conf.override(conf.session_configs.__name__, json.loads(rest_of_line))
            # add
            elif subcommand == "add":
                if len(args.command) != 4 and len(args.command) != 5:
                    raise ValueError("Subcommand 'add' requires three or four arguments.\n{}".format(usage))

                name = args.command[1].lower()
                language = args.command[2].lower()
                connection_string = args.command[3]

                if len(args.command) == 5:
                    skip = args.command[4].lower() == "skip"
                else:
                    skip = False

                properties = copy.deepcopy(conf.session_configs())
                properties["kind"] = self._get_livy_kind(language)

                self.spark_controller.add_session(name, connection_string, skip, properties)
            # delete
            elif subcommand == "delete":
                if len(args.command) == 2:
                    name = args.command[1].lower()
                    self.spark_controller.delete_session_by_name(name)
                elif len(args.command) == 3:
                    connection_string = args.command[1]
                    session_id = args.command[2]
                    self.spark_controller.delete_session_by_id(connection_string, session_id)
                else:
                    raise ValueError("Subcommand 'delete' requires a session name or a connection string and id.\n{}"
                                     .format(usage))
            # cleanup
            elif subcommand == "cleanup":
                if len(args.command) == 2:
                    connection_string = args.command[1]
                    self.spark_controller.cleanup_endpoint(connection_string)
                elif len(args.command) == 1:
                    self.spark_controller.cleanup()
                else:
                    raise ValueError("Subcommand 'cleanup' requires no further values or a connection string to clean up "
                                     "sessions.\n{}".format(usage))
            # logs
            elif subcommand == "logs":
                if len(args.command) == 1:
                    (success, out) = self.spark_controller.get_logs(args.session)
                    if success:
                        self.ipython_display.write(out)
                    else:
                        self.ipython_display.send_error(out)
                else:
                    raise ValueError("Subcommand 'logs' requires no further values.\n{}".format(usage))
            # run
            elif len(subcommand) == 0:
                if args.context == Constants.context_name_spark:
                    (success, out) = self.spark_controller.run_cell(cell, args.session)
                    if success:
                        self.ipython_display.write(out)
                    else:
                        self.ipython_display.send_error(out)
                elif args.context == Constants.context_name_sql:
                    return self._execute_against_context_that_returns_df(self.spark_controller.run_cell_sql, cell,
                                                                         args.session, args.output)
                elif args.context == Constants.context_name_hive:
                    return self._execute_against_context_that_returns_df(self.spark_controller.run_cell_hive, cell,
                                                                         args.session, args.output)
                else:
                    raise ValueError("Context '{}' not found".format(args.context))
            # error
            else:
                raise ValueError("Subcommand '{}' not found. {}".format(subcommand, usage))
        except ValueError as err:
            self.ipython_display.send_error("{}".format(err))

    def _execute_against_context_that_returns_df(self, method, cell, session, output_var):
        try:
            df = method(cell, session)
            if output_var is not None:
                self.shell.user_ns[output_var] = df
            return df
        except DataFrameParseException as e:
            self.ipython_display.send_error(e.out)
            return None

    def _print_local_info(self):
        sessions_info = ["        {}".format(i) for i in self.spark_controller.get_manager_sessions_str()]
        print("""Info for running Spark:
    Sessions:
{}
    Session configs:
        {}
""".format("\n".join(sessions_info), conf.session_configs()))


    def _print_endpoint_info(self, info_sessions):
        sessions_info = ["        {}".format(i) for i in info_sessions]
        print("""Info for endpoint:
    Sessions:
{}
""".format("\n".join(sessions_info)))

    @staticmethod
    def _get_livy_kind(language):
        if language == Constants.lang_scala:
            return Constants.session_kind_spark
        elif language == Constants.lang_python:
            return Constants.session_kind_pyspark
        elif language == Constants.lang_r:
            return Constants.session_kind_sparkr
        else:
            raise ValueError("Cannot get session kind for {}.".format(language))

        
def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
