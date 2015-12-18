"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class, line_cell_magic, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from .livyclientlib.sparkcontroller import SparkController
from .livyclientlib.log import Log
from .livyclientlib.utils import get_magics_home_path, join_paths
from .livyclientlib.configuration import get_configuration
from .livyclientlib.constants import Constants
from .livyclientlib.dataframeparseexception import DataFrameParseException


@magics_class
class RemoteSparkMagics(Magics):
    def __init__(self, shell, data=None):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)

        self.logger = Log("RemoteSparkMagics")
        self.spark_controller = SparkController()

        try:
            should_serialize = get_configuration(Constants.serialize, False)
            if should_serialize:
                self.logger.debug("Serialization enabled.")

                self.magics_home_path = get_magics_home_path()
                path_to_serialize = join_paths(self.magics_home_path, "state.json")

                self.logger.debug("Will serialize to {}.".format(path_to_serialize))

                self.spark_controller = SparkController(serialize_path=path_to_serialize)
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
    def spark(self, line, cell="", local_ns={}):
        """Magic to execute spark remotely.

           This magic allows you to create a Livy Scala or Python session against a Livy endpoint. Every session can
           be used to execute either Spark code or SparkSQL code by executing against the SQL context in the session.
           When the SQL context is used, the result will be a Pandas dataframe of a sample of the results.

           If invoked with no subcommand, the cell will be executed against the specified session.
           Subcommands
           -----------
           info
               Display the mode and available Livy sessions.
           add
               Add a Livy session. First argument is the name of the session, second argument
               is the language, and third argument is the connection string of the Livy endpoint.
               A fourth argument specifying if session creation can be skipped if it already exists is optional:
               "skip" or empty.
               e.g. `%%spark add test python url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword skip`
               or
               e.g. `%%spark add test python url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword`
           run
               Run Spark code against a session.
               e.g. `%%spark -e testsession` will execute the cell code against the testsession previously created
               e.g. `%%spark -e testsession -c sql` will execute the SQL code against the testsession previously created
               e.g. `%%spark -e testsession -c sql -o my_var` will execute the SQL code against the testsession previously
                        created and store the pandas dataframe created in the my_var variable in the Python environment
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

        # info
        if subcommand == "info":
            self._print_info()
        # add
        elif subcommand == "add":
            if len(args.command) != 4 and len(args.command) != 5:
                raise ValueError("Subcommand 'add' requires three or four arguments. {}".format(usage))
            name = args.command[1].lower()
            language = args.command[2]
            connection_string = args.command[3]
            if len(args.command) == 5:
                skip = args.command[4].lower() == "skip"
            else:
                skip = False
            self.spark_controller.add_session(name, language, connection_string, skip)
        # delete
        elif subcommand == "delete":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'delete' requires an argument. {}".format(usage))
            name = args.command[1].lower()
            self.spark_controller.delete_session(name)
        # cleanup 
        elif subcommand == "cleanup":
            self.spark_controller.cleanup()
        # run
        elif len(subcommand) == 0:
            if args.context == Constants.context_name_spark:
                (success, out) = self.spark_controller.run_cell(cell, args.session)
                if success:
                    self.shell.write(out)
                else:
                    self.shell.write_err(out)
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

    def _execute_against_context_that_returns_df(self, method, cell, session, output_var):
        try:
            df = method(cell, session)
            if output_var is not None:
                self.shell.user_ns[output_var] = df
            return df
        except DataFrameParseException as e:
            self.shell.write_err(e.out)
            return None

    def _print_info(self):
        print("Info for running Spark:\n\t{}\n".format(self.spark_controller.get_client_keys()))

        
def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
