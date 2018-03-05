import re
import os

from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope, cell_magic, line_magic
from IPython.core.magic_arguments import argument, magic_arguments

from .base import ThriftMagicBase

from sparkmagic.utils.utils import parse_argstring_or_throw, get_coerce_value
from sparkmagic.utils.constants import THRIFT_VAR, THRIFT_LOG_VAR

from sparkmagic.livyclientlib.exceptions import handle_expected_exceptions, wrap_unexpected_exceptions
from sparkmagic.utils.thriftlogger import ThriftLog
import sparkmagic.utils.configuration as conf
from sparkmagic.thriftclient.thriftexceptions import ThriftConfigurationError

@magics_class
class ThriftKernelMagics(ThriftMagicBase):
    def __init__(self, shell):
        self.logger = ThriftLog(self.__class__, conf.logging_config_debug())
        super(ThriftKernelMagics, self).__init__(shell)
        self.logger.debug("Initialized '{}'".format(self.__class__))

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=THRIFT_VAR, help="If present, query will be stored in variable of this "
                                                             "name.")
    @argument("-l", "--logs", type=str, default=THRIFT_LOG_VAR, help="If present, logs will be stored in variable of this "
                                                             "name.")
    @argument("-q", "--quiet", type=bool, default=False, const=True, nargs="?", help="Return None instead of the dataframe.")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for SQL queries: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the server for SQL queries")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from SQL queries")
    @argument("-c", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) "
                                                                        "of the dataframe or not (pass False)")
    #@wrap_unexpected_exceptions
    #@handle_expected_exceptions
    def sql(self, line, cell="", local_ns=None):
        args = parse_argstring_or_throw(self.sql, line)

        coerce = get_coerce_value(args.coerce)

        # If just typing one word -> assume it is meant as a local variable and try to return that
        if len(cell.split()) == 1:
            try:
                uservar = self.shell.user_ns[cell]
                self.magic_writeln(str(uservar))
                return None
            except KeyError as ke:
                self.magic_writeln('Could not find {!r} in user variables'.format(cell))
                self.magic_writeln('Attempting to execute {!r} as a query...'.format(cell))

        return self.execute_sqlquery(cell, args.samplemethod, args.maxrows, args.samplefraction,
                                     args.output, args.logs, args.quiet, coerce)


    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-c", "--config", type=str, default=None, help="Specify a new config in cell body")
    @argument("-f", "--fileconfig", type=str, default=None, help="Specify a new config file to use")
    @argument("-r", "--restore", type=str, default=None, help="Restore to default conf")
    @argument("-p", "--print", type=str, default=None, help="Print current connection info")
    #@wrap_unexpected_exceptions
    #@handle_expected_exceptions
    def sqlconfig(self, line, cell="", local_ns=None):
        """
        Specify or refresh connection information

        Has three modes:
            * TODO: No arguments, No cell body -> display input boxes with default values
            * File input -> grab input from file
            * Configuration input -> grab input from cell
            * Can also restor to start-up config or print current configuration

        Expects dictionary format:

        host: '<host>'
        port: <port>
        conf: <configuration_dict>
        set key=var

        Note:
        * Non-recognized kewords throws an error
        * Missing keywords are not overwritten
        * key=vars are automatically appended into conf
        * conf: overwrites entire current conf
        * configuration_dict should be specified as {key1: 'str_val', key2: int_val}
        """

        # Show a widget with values to user for inputting the different configurations
        if line.strip() == sqlconfig.__name__:
            pass


        # Print connection details to screen
        if "-p" in line or "--print" in line:
            if self.thriftcontroller.connection:
                self.magic_writeln(str(self.thriftcontroller.connection))
            else:
                self.magic_send_error(r"No connection detected!\nTry refreshing with %sqlrefresh")
            return

        # Restore connection details to defaults
        if "-r" in line or "--restore" in line:
            try:
                self.thriftcontroller.reset_defaults()
            except ThriftConfigurationError as tce:
                self.magic_send_error("Could not restore to default settings")
                self.magic_send_error(str(tce))
            else:
                self.magic_writeln("Restored to default settings:\n{}".format(self.thriftcontroller.connection))
            return


        newsettings = None
        filenamechars = r"[0-9a-zA-Z_\-./+=\\]"
        # Get configuration from file
        if "-f" in line or "--fileconfig" in line:
            # Find file in line input and whether it exists
            fileconfig = re.findall(r'-f ({}+)'.format(filenamechars), line)
            if not fileconfig:
                fileconfig = re.findall(r'--fileconfig ({}+)'.format(filenamechars), line)
            if not fileconfig:
                self.magic_send_error("Could not parse out a fileconfig file in input line: {}".format(line))
                return
            fileconfig = fileconfig[0]
            self.logger.debug("Using file connection details {!r}".format(fileconfig))
            if os.path.isfile(fileconfig):
                with open(fileconfig, 'r') as f:
                    newsettings = "".join(f.readlines())
                    self.logger.debug("Found settings in file: {}".format(newsettings))
            else:
                self.magic_send_error("Could not locate file {!r}\nIgnoring command...".format(fileconfig))
                return

        # Get configuration from code
        if "-c" in line or "--config" in line:
            self.logger.debug("Using cell connection details {!r}".format(cell))
            newsettings = cell

        # Parse the collected settings
        if newsettings is not None:
            self.magic_writeln("Setting new connection...")
            settings_list = []
            for ms in newsettings.split('\n'):
                mss = ms.strip()
                # Kewords set with hive syntax 'set k=v' or regular k:v
                if mss and mss.lower().startswith("set"):
                    key, val = ms.strip().split('=',1)
                    # Users shouldn't, but if they do use hivevar or hiveconf, remove it
                    stripped_key = re.sub(r'hivevar:|hiveconf:','','set hiveconff:k=v')
                    settings_list.append(key, val)
                elif mss:
                    settings_list.append(ms.strip().split(':',1))
                if settings_list and len(settings_list[-1]) != 2:
                    self.magic_send_error("Failed parsing input variables {!r}".format(mss))
                    self.magic_send_error("Make sure each setting is on a separate line with either set k=v, or k:v format")
                    return
            # Strip away all spaces, newlines and tabs
            settings = {key.strip(): val.strip() for key,val in settings_list}
            self.logger.debug("New settings {}".format(settings))
            try:
                self.thriftcontroller.set_conf(settings)
            except ThriftConfigurationError as tce:
                self.magic_send_error(str(tce))
                self.magic_send_error("Connection not updated")
            else:
                self.magic_writeln("Thrift connection updated, current connection is:\n{}".format(self.thriftcontroller.connection))
        else:
            self.magic_writeln("No action since {!r} usage pattern is not supported".format(line))


    def sqlmetaconf():
        """
                metastore_host: <host>
                metastore_port: <port>
        """
        pass

    @cell_magic
    def sqlconnect(self, line, cell="", local_ns=None):
        """
        Connects without executing query
        """
        try:
            self.thriftcontroller.connect()
        except ThriftConfigurationError as tce:
            self.magic_send_error("Failed connecting to thriftserver")
            self.magic_send_error(str(tce))
        else:
            self.magic_writeln("Connected to thriftserver")

    @cell_magic
    def sqlrefresh(self, line, cell="", local_ns=None):
        """
        Stops and restarts the current connection cursor
        """
        self._sqlrefresh()

    def _sqlrefresh(self):
        try:
            self.thriftcontroller.reset()
        except ThriftConfigurationError as tce:
            self.magic_send_error("Failed resetting connection")
            self.magic_send_error(str(tce))
        else:
            self.magic_writeln("Thrift connection reset")

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-r", "--refresh", type=str, default=None, help="If present, resets and reconnect to thriftserver")
    @argument("-c", "--config", type=str, default=None, help="Specify a new config in cell body")
    @argument("-p", "--print", type=str, default=None, help="Print current connection info")
    @argument("-f", "--fileconfig", type=str, default=None, help="Specify a new config file to use")
    #@wrap_unexpected_exceptions
    #@handle_expected_exceptions
    def sqlconfig_hive(self, line, cell="", local_ns=None):
        """
        Has three modes:
            No arguments, No cell body -> refresh configuration

        """
        pass

    def magic_send_error(self, msg):
        self.logger.error(msg)
        self.ipython_display.send_error(msg)

    def magic_writeln(self, msg):
        self.logger.info(msg)
        self.ipython_display.writeln(msg)


    @cell_magic
    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def ls_sqlmagics(self, line, cell="", local_ns=None):
        self.magic_writeln(' '.join("%{}".format(m) for m in sorted(i.__name__ for i in self.magicfunctions)))

    magicfunctions = [sqlrefresh, sqlconnect, ls_sqlmagics, sql, sqlconfig]

def load_ipython_extension(ip):
    ip.register_magics(ThriftKernelMagics)
