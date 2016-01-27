"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import magics_class
from IPython.core.magic import line_cell_magic, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
import json
import copy

import remotespark.utils.configuration as conf
from remotespark.utils.constants import Constants
from remotespark.magics.sparkmagicsbase import SparkMagicBase
from remotespark.utils.utils import get_connection_string


@magics_class
class KernelMagics(SparkMagicBase):
    def __init__(self, shell, data=None):
        # You must call the parent constructor
        super(KernelMagics, self).__init__(shell, data)

        # Overwrite
        self.language = Constants.lang_python

        self.session_name = "session_name"
        self.session_started = False

        configuration = self._get_configuration()
        if not configuration:
            # _get_configuration() sets the error for us so we can just return now.
            # The kernel is not in a good state and all do_execute calls will
            # fail with the fatal error.
            return
        (username, password, url) = configuration
        self.connection_string = get_connection_string(url, username, password)

    @line_cell_magic
    def info(self, line, cell="", local_ns=None):
        info_sessions = self.spark_controller.get_all_sessions_endpoint_info(self.connection_string)
        self._print_endpoint_info(info_sessions)

    @line_cell_magic
    def logs(self, line, cell="", local_ns=None):
        (success, out) = self.spark_controller.get_logs()
        if success:
            self.ipython_display.write(out)
        else:
            self.ipython_display.send_error(out)

    @magic_arguments()
    @line_cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @argument("settings", type=str, default=[""], nargs="*", help="Settings to configure session with.")
    def config(self, line, cell="", local_ns=None):
        args = parse_argstring(self.config, line)

        if self.session_started:
            if not args.force:
                raise ValueError("A session has already been started. If you intend to recreate the session with "
                                 "new configurations, please include the -f argument.")
            else:
                self._delete_session()
                self._override_session_settings(args.settings)
                self._start_session()

        self._override_session_settings(args.settings)

    @line_cell_magic
    def spark(self, line, cell="", local_ns=None):
        self._start_session("")

        (success, out) = self.spark_controller.run_cell(cell)
        if success:
            self.ipython_display.write(out)
        else:
            self.ipython_display.send_error(out)

    @magic_arguments()
    @line_cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, query will be stored in variable of this "
                                                             "name.")
    def sql(self, line, cell="", local_ns=None):
        args = parse_argstring(self.sql, line)
        return self._execute_against_context_that_returns_df(self.spark_controller.run_cell_sql, cell,
                                                             None, args.output)

    @magic_arguments()
    @line_cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, query will be stored in variable of this "
                                                             "name.")
    def hive(self, line, cell="", local_ns=None):
        args = parse_argstring(self.hive, line)
        return self._execute_against_context_that_returns_df(self.spark_controller.run_cell_hive, cell,
                                                             None, args.output)

    @magic_arguments()
    @line_cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    def cleanup(self, line, cell="", local_ns=None):
        args = parse_argstring(self.cleanup, line)
        if args.force:
            self._delete_session("")

            self.spark_controller.cleanup_endpoint(self.connection_string)

    @magic_arguments()
    @line_cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @argument("-s", "--session", type=int, help="Session id number to delete.")
    def delete(self, line, cell="", local_ns=None):
        args = parse_argstring(self.delete, line)
        if args.force:
            if args.session == self.spark_controller.get_session_id_for_client(self.session_name):
                raise ValueError("Cannot delete this kernel's session. Specify a different session, shutdown the "
                                 "kernel to delete this session, or run %cleanup to delete all sessions for this "
                                 "endpoint.")

            self.spark_controller.delete_session_by_id(self.connection_string, args.session)

    @line_cell_magic
    def _start_session(self, line, cell="", local_ns=None):
        if not self.session_started:
            self.session_started = True

            skip = False
            properties = copy.deepcopy(conf.session_configs())
            properties["kind"] = self._get_livy_kind(self.language)

            self.spark_controller.add_session(self.session_name, self.connection_string, skip, properties)

    @line_cell_magic
    def _delete_session(self, line, cell="", local_ns=None):
        if self.session_started:
            self.spark_controller.delete_session_by_name(self.session_name)
            self.session_started = False

    def _get_configuration(self):
        """Returns (username, password, url). If there is an error (missing configuration),
           returns False."""
        credentials = getattr(conf, 'kernel_' + self.language + '_credentials')()
        ret = (credentials['username'], credentials['password'], credentials['url'])
        # The URL has to be set in the configuration.
        assert(ret[2])
        return ret

    @staticmethod
    def _override_session_settings(settings):
        conf.override(conf.session_configs.__name__, json.loads(" ".join(settings)))


def load_ipython_extension(ip):
    ip.register_magics(KernelMagics)
