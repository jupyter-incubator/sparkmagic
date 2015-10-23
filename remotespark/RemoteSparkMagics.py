"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

from IPython.core.magic import (Magics, magics_class, line_cell_magic)
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)

from .livyclientlib.sparkcontroller import SparkController
from .livyclientlib.log import Log


@magics_class
class RemoteSparkMagics(Magics):

    logger = Log()

    def __init__(self, shell, data=None, mode="normal"):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)
        self.spark_controller = SparkController(mode)

    @magic_arguments()
    @argument("-s", "--sql", type=bool, default=False, help='Whether to use SQL.')
    @argument("-c", "--client", help="The name of the Livy client to use. "
              "If only one client has been created, there's no need to specify a client.")
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @line_cell_magic
    def spark(self, line, cell=""):
        """Magic to execute spark remotely.           
           If invoked with no subcommand, the code will be executed against the specified endpoint.
           Subcommands
           -----------
           info
               Display the mode and available Livy endpoints.
           mode
               Set the mode to be used. Possible arguments are: "normal" or "debug".
               e.g. `%%spark mode debug`
           add
               Add a Livy endpoint. First argument is the friendly name of the endpoint, second argument
               is the language, and third argument is the connection string.
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
        # mode
        elif subcommand == "mode":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'mode' requires an argument. {}".format(usage))
            self.spark_controller.set_log_mode(args.command[1])
        # add
        elif subcommand == "add":
            if len(args.command) != 4:
                raise ValueError("Subcommand 'add' requires three arguments. {}".format(usage))
            name = args.command[1].lower()
            language = args.command[2]
            connection_string = args.command[3]
            self.spark_controller.add_endpoint(name, language, connection_string)
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
            self.logger.debug("line: " + line)
            self.logger.debug("cell: " + cell)
            self.logger.debug("args: " + str(args))
            return self.spark_controller.run_cell(args.client, args.sql, cell)
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
        print("Info for running Spark:\n    mode={}\n    {}\n"
              .format(self.spark_controller.get_log_mode(), self.spark_controller.get_client_keys()))

        
def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
