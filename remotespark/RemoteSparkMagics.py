"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
import warnings

from IPython.core.magic import (Magics, magics_class, line_cell_magic)
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)

from .livyclientlib.sparkcontroller import SparkController
from .livyclientlib.log import Log
from .livyclientlib.utils import get_magics_home_path, join_paths
from .livyclientlib.configuration import get_configuration
from .livyclientlib.constants import Constants

@magics_class
class RemoteSparkMagics(Magics):

    def __init__(self, shell, data=None, test=False):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)

        use_auto_viz = get_configuration(Constants.use_auto_viz, True) and not test
        self.interactive = get_configuration(Constants.display_info, False)

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
    @argument("-e", "--endpoint", help="The name of the Livy endpoint to use. "
                                       "If only one endpoint has been created, there's no need to specify one.")
    @argument("-t", "--chart", type=str, default="area", help='Chart type to use: table, area, line, bar.')
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @line_cell_magic
    def spark(self, line, cell=""):
        """Magic to execute spark remotely.           
           If invoked with no subcommand, the code will be executed against the specified endpoint.
           Subcommands
           -----------
           info
               Display the mode and available Livy endpoints.
           add
               Add a Livy endpoint. First argument is the friendly name of the endpoint, second argument
               is the language, and third argument is the connection string. A fourth argument specifying if
               endpoint can be skipped if already present is optional: "skip" or empty.
               e.g. `%%spark add test python url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword skip`
               or
               e.g. `%%spark add test python url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword`
           delete
               Delete a Livy endpoint. Argument is the friendly name of the endpoint to be deleted.
               e.g. `%%spark delete defaultlivy`
           cleanup
               Delete all Livy endpoints. No arguments required.
               e.g. `%%spark cleanup`
        """
        usage = "Please look at usage of %spark by executing `%spark?`."
        user_input = line
        args = parse_argstring(self.spark, user_input)

        subcommand = args.command[0].lower()

        # info
        if subcommand == "info":
            # Info is printed by default
            pass
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
            self.spark_controller.add_endpoint(name, language, connection_string, skip)
        # delete
        elif subcommand == "delete":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'delete' requires an argument. {}".format(usage))
            name = args.command[1].lower()
            self.spark_controller.delete_endpoint(name)
        # cleanup 
        elif subcommand == "cleanup":
            self.spark_controller.cleanup()
        # run
        elif len(subcommand) == 0:
            if args.context == Constants.context_name_spark:
                return self.spark_controller.run_cell(cell, args.endpoint)
            elif args.context == Constants.context_name_sql:
                return self.spark_controller.run_cell_sql(cell, args.endpoint)
            elif args.context == Constants.context_name_hive:
                return self.spark_controller.run_cell_hive(cell, args.endpoint)
            else:
                raise ValueError("Context '{}' not found".format(args.context))
        # error
        else:
            raise ValueError("Subcommand '{}' not found. {}".format(subcommand, usage))

        # Print info after any valid subcommand
        if len(subcommand) > 0:
            if len(cell) > 0:
                print("Warning: Cell body not executed because subcommmand found.")
                print(usage)
            self._print_info()

    def _print_info(self):
        if self.interactive:
            print("Info for running Spark:\n\t{}\n".format(self.spark_controller.get_client_keys()))

        
def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
