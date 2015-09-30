"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %sparkmagic, %sparkconf magics."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

from IPython.core.magic import (Magics, magics_class, line_magic, line_cell_magic)
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)

from livyclientlib.clientmanager import ClientManager
from livyclientlib.livyclientfactory import LivyClientFactory
from livyclientlib.log import Log
from livyclientlib.constants import Constants


@magics_class
class RemoteSparkMagics(Magics):

    logger = Log()

    def __init__(self, shell, data=None, mode="normal"):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)
        Log.mode = mode
        self.client_manager = ClientManager()
        self.client_factory = LivyClientFactory()

    @magic_arguments()
    @argument("-s", "--sql", type=bool, default=False, help='Whether to use SQL.')
    @argument("-m", "--mode", help='The mode to execute the magic in: "normal" or "debug". Default is "normal".')
    @argument("-c", "--client", help="The name of the Livy client to use. "
              "Add a session by using %sparkconfig. "
              "If only one client has been created, there's no need to specify a client.")
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @line_cell_magic
    def sparkmagic(self, line, cell=""):
        """Magic to do remote execution of Spark code.
           Arguments should be in line while code should be in cell."""
        user_input = line
        args = parse_argstring(self.sparkmagic, user_input)
        
        # Change mode
        previous_mode = Log.mode
        if args.mode:
            Log.mode = args.mode

        # Consolidate commands
        command = cell
        
        self.logger.debug("line: " + line)
        self.logger.debug("cell: " + cell)
        self.logger.debug("args: " + str(args))
        self.logger.debug("command: " + command)

        # Select client
        if not args.client:
            client_to_use = self.client_manager.get_any_client()
        else:
            args.client = args.client.lower()
            client_to_use = self.client_manager.get_client(args.client)

        # Execute
        print(self._send_command(client_to_use, command, args.sql))

        # Revert mode
        Log.mode = previous_mode

    @magic_arguments()
    @argument("command", type=str, default=[""], nargs="*", help="Command to execute.")
    @line_magic
    def sparkconf(self, line, cell=None):
        """Magic to configure remote spark usage.
           
           Usage
           -----

               %sparkconf [subcommand] [arg]
           
           If invoked with no subcommand, subcommand info will be assumed.

           Subcommands
           -----------

           info
               Display the mode and available Livy endpoints.
           mode
               Set the mode to be used. Possible arguments are: "normal" or "debug".
               e.g. `%sparkconf mode debug`
           add
               Add a Livy endpoint. First argument is the friendly name of the endpoint, second argument
               is the language, and third argument is the connection string.
               e.g. `%sparkconf add test python url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword`
           delete
               Delete a Livy endpoint. Argument is the friendly name of the endpoint to be deleted.
               e.g. `%sparkconf delete defaultlivy`
           cleanup
               Delete all Livy endpoints. No arguments required.
               e.g. `%sparkconf cleanup`
        """
        usage = "Please look at usage of sparkconf by executing `%sparkconf?`."
        args = parse_argstring(self.sparkconf, line)

        # Select subcommand
        if args.command[0] == "":
            subcommand = "info"
        else:
            subcommand = args.command[0].lower()

        # info
        if subcommand == "info":
            self._print_info()
        # mode
        elif subcommand == "mode":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'mode' requires an argument. {}".format(usage))
            Log.mode = args.command[1]
            self._print_info()
        # add
        elif subcommand == "add":
            if len(args.command) != 4:
                raise ValueError("Subcommand 'add' requires three arguments. {}".format(usage))
            name = args.command[1].lower()
            language = args.command[2]
            connection_string = args.command[3]

            livy_client = self.client_factory.build_client(connection_string, language)

            self.client_manager.add_client(name, livy_client)

            self._print_info()
        # delete
        elif subcommand == "delete":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'delete' requires an argument. {}".format(usage))

            name = args.command[1].lower()
            self.client_manager.delete_client(name)

            self._print_info()
        # delete
        elif subcommand == "cleanup":
            self.client_manager.clean_up_all()

            self._print_info()
        # error
        else:
            raise ValueError("Subcommand '{}' not supported. {}".format(subcommand, usage))

    def _print_info(self):
        print("Info for running sparkmagic:\n    mode={}\n    {}\n".format(Log.mode, self._get_client_keys()))

    def _get_client_keys(self):
        return "Possible endpoints are: {}".format(self.client_manager.get_endpoints_list())

    def _send_command(self, client, command, sql):
        if sql:
            res = client.execute_sql(command)
        else:
            res = client.execute(command)
        return res
        

def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
