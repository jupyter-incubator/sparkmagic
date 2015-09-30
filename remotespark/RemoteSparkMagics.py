"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %sparkmagic, %sparkconf magics."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

from IPython.core.magic import (Magics, magics_class, line_magic, line_cell_magic)
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)

from .livyclientlib.clientmanager import ClientManager
from .livyclientlib.livyclientfactory import LivyClientFactory
from .livyclientlib.log import Log
from .livyclientlib.constants import Constants


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
    @argument("-c", "--client", help="The name of the Livy client to use. "
              "Add a session by using %sparkconfig. "
              "If only one client has been created, there's no need to specify a client.")
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @line_cell_magic
    def spark(self, line, cell=""):
        """Magic to do remote execution of Spark code.
           Arguments should be in line while code should be in cell."""
        usage = "Please look at usage of sparkconf by executing `%sparkconf?`."
        user_input = line
        args = parse_argstring(self.spark, user_input)


        subcommand = args.command[0].lower()

        # info
        if subcommand == "info":
            pass #info is printed by default
        # mode
        elif subcommand == "mode":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'mode' requires an argument. {}".format(usage))
            self.log_mode(args.command[1])
        # add
        elif subcommand == "add":
            if len(args.command) != 4:
                raise ValueError("Subcommand 'add' requires three arguments. {}".format(usage))
            name = args.command[1].lower()
            language = args.command[2]
            connection_string = args.command[3]
            self.add_endpoint(name, language, connection_string)
        # delete
        elif subcommand == "delete":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'delete' requires an argument. {}".format(usage))
            name = args.command[1].lower()
            self.delete_endpoint(name)  
        # cleanup 
        elif subcommand == "cleanup":
            self.cleanup()
        # run
        elif len(subcommand) == 0:    
            self.logger.debug("line: " + line)
            self.logger.debug("cell: " + cell)
            self.logger.debug("args: " + str(args))
            self.run_cell(args.client, args.sql, cell)
        #error
        else:
            raise ValueError("Subcommand '{}' not found. {}".format(subcommand, usage))
        
        # Print info after any valid subcommand
        if len(subcommand) > 0:
            self._print_info()

    def run_cell(self, client_name, sql, cell):
        # Select client
        if not client_name:
            client_to_use = self.client_manager.get_any_client()
        else:
            args.client = args.client.lower()
            client_to_use = self.client_manager.get_client(client_name)

        # Execute
        print(self._send_command(client_to_use, cell, sql))

    def cleanup(self):
        self.client_manager.clean_up_all()

    def log_mode(self, mode):
        Log.mode = mode

    def delete_endpoint(self, name):
        self.client_manager.delete_client(name)

    def add_endpoint(self, name, language, connection_string):
        livy_client = self.client_factory.build_client(connection_string, language)
        self.client_manager.add_client(name, livy_client)

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
