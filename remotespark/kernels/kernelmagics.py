"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import magics_class
from IPython.core.magic import needs_local_scope, cell_magic
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

        # Overwrite if needed
        self.language = Constants.lang_python

        self.session_name = "session_name"
        self.session_started = False

        (username, password, url) = self.get_configuration(self.language)
        self.connection_string = get_connection_string(url, username, password)

    @cell_magic
    def local(self, line, cell="", local_ns=None):
        # This should not be reachable thanks to UserCodeParser. Registering it here so that it auto-completes with tab.
        raise NotImplementedError("UserCodeParser should have prevented code execution from reaching here.")

    @cell_magic
    def info(self, line, cell="", local_ns=None):
        info_sessions = self.spark_controller.get_all_sessions_endpoint_info(self.connection_string)
        self.print_endpoint_info(info_sessions)

    @cell_magic
    def logs(self, line, cell="", local_ns=None):
        if self.session_started:
            (success, out) = self.spark_controller.get_logs()
            if success:
                self.ipython_display.write(out)
            else:
                self.ipython_display.send_error(out)
        else:
            self.ipython_display.write("No logs yet.")

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @argument("settings", type=str, default=[""], nargs="*", help="Settings to configure session with.")
    def configure(self, line, cell="", local_ns=None):
        args = parse_argstring(self.configure, line)

        # We ignore args.settings because argparse removes quotes. Instead, we get them ourselves.
        settings = self.get_session_settings(line, args.force)

        if self.session_started:
            if not args.force:
                raise ValueError("A session has already been started. If you intend to recreate the session with "
                                 "new configurations, please include the -f argument.")
            else:
                self._do_not_call_delete_session("")
                self._override_session_settings(settings)
                self._do_not_call_start_session("")
        else:
            self._override_session_settings(settings)

    @cell_magic
    def spark(self, line, cell="", local_ns=None):
        self._do_not_call_start_session("")

        (success, out) = self.spark_controller.run_cell(cell)
        if success:
            self.ipython_display.write(out)
        else:
            self.ipython_display.send_error(out)

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, query will be stored in variable of this "
                                                             "name.")
    def sql(self, line, cell="", local_ns=None):
        args = parse_argstring(self.sql, line)
        return self.execute_against_context_that_returns_df(self.spark_controller.run_cell_sql, cell,
                                                            None, args.output)

    @magic_arguments()
    @cell_magic
    @needs_local_scope
    @argument("-o", "--output", type=str, default=None, help="If present, query will be stored in variable of this "
                                                             "name.")
    def hive(self, line, cell="", local_ns=None):
        args = parse_argstring(self.hive, line)
        return self.execute_against_context_that_returns_df(self.spark_controller.run_cell_hive, cell,
                                                            None, args.output)

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    def cleanup(self, line, cell="", local_ns=None):
        args = parse_argstring(self.cleanup, line)
        if args.force:
            self._do_not_call_delete_session("")

            self.spark_controller.cleanup_endpoint(self.connection_string)
        else:
            raise ValueError("When you clean up the endpoint, all sessions will be lost, including the one used for "
                             "this notebook. Include the -f parameter if that's your intention.")

    @magic_arguments()
    @cell_magic
    @argument("-f", "--force", type=bool, default=False, nargs="?", const=True, help="If present, user understands.")
    @argument("-s", "--session", type=str, nargs=1, help="Session id number to delete.")
    def delete(self, line, cell="", local_ns=None):
        args = parse_argstring(self.delete, line)
        session = args.session[0]

        if args.force:
            id = self.spark_controller.get_session_id_for_client(self.session_name)
            if session == id:
                raise ValueError("Cannot delete this kernel's session ({}). Specify a different session, shutdown the "
                                 "kernel to delete this session, or run %cleanup to delete all sessions for this "
                                 "endpoint.".format(id))

            self.spark_controller.delete_session_by_id(self.connection_string, session)
        else:
            raise ValueError("Include the -f parameter if you understand that all statements executed in this session "
                             "will be lost.")

    @cell_magic
    def _do_not_call_start_session(self, line, cell="", local_ns=None):
        if not self.session_started:
            self.session_started = True

            skip = False
            properties = copy.deepcopy(conf.session_configs())
            properties["kind"] = self.get_livy_kind(self.language)

            self.spark_controller.add_session(self.session_name, self.connection_string, skip, properties)

    @cell_magic
    def _do_not_call_delete_session(self, line, cell="", local_ns=None):
        if self.session_started:
            self.spark_controller.delete_session_by_name(self.session_name)
            self.session_started = False

    @magic_arguments()
    @cell_magic
    @argument("-l", "--language", type=str, help="Language to use.")
    def _do_not_call_change_language(self, line, cell="", local_ns=None):
        args = parse_argstring(self._do_not_call_change_language, line)
        language = args.language.lower()

        if language not in Constants.lang_supported:
            raise ValueError("'{}' language not supported in kernel magics.".format(language))

        assert not self.session_started

        self.language = language

    @staticmethod
    def get_configuration(language):
        """Returns (username, password, url)."""
        credentials = getattr(conf, 'kernel_' + language + '_credentials')()
        ret = (credentials['username'], credentials['password'], credentials['url'])
        # The URL has to be set in the configuration.
        assert(ret[2])
        return ret

    @staticmethod
    def get_session_settings(line, force):
        line = line.strip()
        if not force:
            return line
        else:
            if line.startswith("-f "):
                return line[3:]
            elif line.endswith(" -f"):
                return line[:-3]
            else:
                raise ValueError("Force flag must be at the beginning or end of the line as '-f'.")

    @staticmethod
    def _override_session_settings(settings):
        conf.override(conf.session_configs.__name__, json.loads(settings))


def load_ipython_extension(ip):
    ip.register_magics(KernelMagics)
