"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %remotesparkmode, %remotesparkconfig, %remotesparkinfo, %remotesparksql, %%remotesparkscala, %%remotesparkpyspark magics."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)
from base64 import b64encode

from connectionstringutil import get_connection_string_elements
from livysession import LivySession
from livyclient import LivyClient
from reliablehttpclient import ReliableHttpClient
from log import Log
								
@magics_class
class RemoteSparkMagics(Magics):

    logger = Log()
    livy_clients = dict()

    def __init__(self, shell, data=None, mode="normal"):
        # You must call the parent constructor
        super(RemoteSparkMagics, self).__init__(shell)
        Log.mode = mode

    @magic_arguments()
    @argument("-l", "--language", help='The language to execute: "scala", "pyspark", "sql". Default is "scala".')
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

        # Select language
        if not args.language:
            args.language = "scala"
        args.language = args.language.lower()
        if args.language not in ["scala", "pyspark", "sql"]:
            raise ValueError("Language '{}' not supported.".format(args.language))

        # Select client
        if not args.client:
            number_of_sessions = len(self.livy_clients)
            if number_of_sessions == 1:
                key = self.livy_clients.keys()[0]
                client_to_use = self.livy_clients[key]
            elif number_of_sessions == 0:
                raise SyntaxError("You need to have at least 1 client created to execute commands.")
            else:
                raise SyntaxError("Please specify the client to use. {}".format(self._get_client_keys()))
        else:
            args.client = args.client.lower()
            client_to_use = self.livy_clients[args.client]

        # Execute
        print(self._send_command(client_to_use, command, args.language))

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
               Add a Livy endpoint. First argument is the friendly name of the endpoint and second argument is the connection string.
               e.g. `%sparkconf add defaultlivy url=https://sparkcluster.example.net/livy;username=admin;password=MyPassword`
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
            if len(args.command) != 3:
                raise ValueError("Subcommand 'add' requires two arguments. {}".format(usage))
            name = args.command[1].lower()
            connection_string = args.command[2]

            if name in self.livy_clients.keys():
                raise AssertionError("Endpoint with name '{}' already exists. Please delete the ednpoint first if you intend to replace it.".format(name))

            cso = get_connection_string_elements(connection_string)

            token = b64encode(bytes(cso.username + ":" + cso.password)).decode("ascii")
            headers = {"Content-Type": "application/json", "Authorization": "Basic {}".format(token)}

            http_client = ReliableHttpClient(cso.url, headers)

            spark_session = LivySession(http_client, "spark")
            pyspark_session = LivySession(http_client, "pyspark")

            livy_client = LivyClient(spark_session, pyspark_session)

            self.livy_clients[name] = livy_client

            self._print_info()
        # delete
        elif subcommand == "delete":
            if len(args.command) != 2:
                raise ValueError("Subcommand 'delete' requires an argument. {}".format(usage))

            name = args.command[1].lower()
            self._remove_endpoint(name)

            self._print_info()
        # delete
        elif subcommand == "cleanup":
            for name in self.livy_clients.keys():
                self._remove_endpoint(name)

            self._print_info()
        # error
        else:
            raise ValueError("Subcommand '{}' not supported. {}".format(subcommand, usage))




    def _remove_endpoint(self, name):
        if name in self.livy_clients.keys():
            self.livy_clients[name].close_sessions()
            del self.livy_clients[name]
        else:
            raise ValueError("Could not find '{}' endpoint in list of saved endpoints. {}".format(name, self._get_client_keys()))
        
    def _print_info(self):
        print("Info for running sparkmagic:\n    mode={}\n    {}\n".format(Log.mode, self._get_client_keys()))

    def _get_client_keys(self):
        return "Possible endpoints are: {}".format(self.livy_clients.keys())

    def _send_command(self, client, command, language):
        if language == "scala":
            return client.execute_scala(command)
        elif language == "pyspark":
            return client.execute_pyspark(command)
        elif language == "sql":
            return client.execute_sql(command)
        
def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)