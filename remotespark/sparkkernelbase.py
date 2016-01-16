# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import requests
from ipykernel.ipkernel import IPythonKernel
from remotespark.utils.ipythondisplay import IpythonDisplay

import remotespark.utils.configuration as conf
from remotespark.utils.log import Log
from remotespark.utils.utils import get_connection_string


class SparkKernelBase(IPythonKernel):
    run_command = "run"
    config_command = "config"
    sql_command = "sql"
    hive_command = "hive"
    info_command = "info"
    delete_command = "delete"
    clean_up_command = "cleanup"
    logs_command = "logs"

    force_flag = "f"

    def __init__(self, implementation, implementation_version, language, language_version, language_info,
                 kernel_conf_name, session_language, client_name, **kwargs):
        # Required by Jupyter - Override
        self.implementation = implementation
        self.implementation_version = implementation_version
        self.language = language
        self.language_version = language_version
        self.language_info = language_info

        # Override
        self.kernel_conf_name = kernel_conf_name
        self.session_language = session_language
        self.client_name = client_name

        super(SparkKernelBase, self).__init__(**kwargs)

        self._logger = Log(self.client_name)
        self._session_started = False
        self._fatal_error = None
        self._ipython_display = IpythonDisplay()

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

        if not kwargs.get("testing", False):
            configuration = self._get_configuration()
            if not configuration:
                # _get_configuration() sets the error for us so we can just return now.
                # The kernel is not in a good state and all do_execute calls will
                # fail with the fatal error.
                return
            (username, password, url) = configuration
            self.connection_string = get_connection_string(url, username, password)
            self._load_magics_extension()
            if conf.use_auto_viz():
                self._register_auto_viz()

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if self._fatal_error is not None:
            self._repeat_fatal_error()

        subcommand, flags, code_to_run = self._parse_user_command(code)

        if subcommand == self.run_command:
            code_to_run = "%%spark\n{}".format(code_to_run)
            return self._run_starting_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.sql_command:
            code_to_run = "%%spark -c sql\n{}".format(code_to_run)
            return self._run_starting_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.hive_command:
            code_to_run = "%%spark -c hive\n{}".format(code_to_run)
            return self._run_starting_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.config_command:
            restart_session = False

            if self._session_started:
                if self.force_flag not in flags:
                    self._show_user_error("A session has already been started. In order to modify the Spark configura"
                                           "tion, please provide the '-f' flag at the beginning of the config magic:\n"
                                           "\te.g. `%config -f {}`\n\nNote that this will kill the current session and"
                                           " will create a new one with the configuration provided. All previously run "
                                           "commands in the session will be lost.")
                    code_to_run = ""
                else:
                    restart_session = True
                    code_to_run = "%spark config {}".format(code_to_run)
            else:
                code_to_run = "%spark config {}".format(code_to_run)

            return self._run_restarting_session(code_to_run, silent, store_history, user_expressions, allow_stdin,
                                                restart_session)
        elif subcommand == self.info_command:
            code_to_run = "%spark info {}".format(self.connection_string)
            return self._run_without_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.delete_command:
            if self.force_flag not in flags:
                self._show_user_error("The session you are trying to delete could be this kernel's session. In order "
                                      "to delete this session, please provide the '-f' flag at the beginning of the "
                                      "delete magic:\n\te.g. `%delete -f id`\n\nAll previously run commands in the "
                                      "session will be lost.")
                code_to_run = ""
            else:
                self._session_started = False
                code_to_run = "%spark delete {} {}".format(self.connection_string, code_to_run)

            return self._run_without_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.clean_up_command:
            if self.force_flag not in flags:
                self._show_user_error("The sessions you are trying to delete could be this kernel's session or other "
                                      "people's sessions. In order to delete them, please provide the '-f' flag at the "
                                      "beginning of the cleanup magic:\n\te.g. `%cleanup -f`\n\nAll previously run "
                                      "commands in the sessions will be lost.")
                code_to_run = ""
            else:
                self._session_started = False
                code_to_run = "%spark cleanup {}".format(self.connection_string)

            return self._run_without_session(code_to_run, silent, store_history, user_expressions, allow_stdin)
        elif subcommand == self.logs_command:
            if self.session_started:
                code_to_run = "%%spark logs"
            else:
                code_to_run = "print('No logs yet.')"
            return self._execute_cell(code_to_run, silent, store_history, user_expressions, allow_stdin)
        else:
            self._show_user_error("Magic '{}' not supported.".format(subcommand))
            return self._run_without_session("", silent, store_history, user_expressions, allow_stdin)

    def do_shutdown(self, restart):
        # Cleanup
        self._delete_session()

        return self._do_shutdown_ipykernel(restart)

    def _load_magics_extension(self):
        register_magics_code = "%load_ext remotespark"
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to load the Spark magics library.")
        self._logger.debug("Loaded magics.")

    def _register_auto_viz(self):
        register_auto_viz_code = """from remotespark.datawidgets.utils import display_dataframe
ip = get_ipython()
ip.display_formatter.ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)"""
        self._execute_cell(register_auto_viz_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to register auto viz for notebook.")
        self._logger.debug("Registered auto viz.")

    def _start_session(self):
        if not self._session_started:
            self._session_started = True
            self._ipython_display.writeln('Starting Livy Session')

            add_session_code = "%spark add {} {} {} skip".format(
                self.client_name, self.session_language, self.connection_string)
            self._execute_cell(add_session_code, True, False, shutdown_if_error=True,
                               log_if_error="Failed to create a Livy session.")
            self._logger.debug("Added session.")

    def _delete_session(self):
        if self._session_started:
            code = "%spark cleanup"
            self._execute_cell_for_user(code, True, False)
            self._session_started = False

    def _run_without_session(self, code, silent, store_history, user_expressions, allow_stdin):
        return self._execute_cell(code, silent, store_history, user_expressions, allow_stdin)

    def _run_starting_session(self, code, silent, store_history, user_expressions, allow_stdin):
        self._start_session()
        return self._execute_cell(code, silent, store_history, user_expressions, allow_stdin)

    def _run_restarting_session(self, code, silent, store_history, user_expressions, allow_stdin, restart):
        if restart:
            self._delete_session()

        res = self._execute_cell(code, silent, store_history, user_expressions, allow_stdin)

        if restart:
            self._start_session()

        return res

    def _get_configuration(self):
        """Returns (username, password, url). If there is an error (missing configuration),
           returns False."""
        try:
            credentials = getattr(conf, 'kernel_' + self.kernel_conf_name + '_credentials')()
            ret = (credentials['username'], credentials['password'], credentials['url'])
            # The URL has to be set in the configuration.
            assert(ret[2])
            return ret
        except (KeyError, AssertionError):
            message = "Please set configuration for 'kernel_{}_credentials' to initialize Kernel".format(
                self.kernel_conf_name)
            self._queue_fatal_error(message)
            return False

    def _parse_user_command(self, code):
        # Normalize 2 signs to 1
        if code.startswith("%%"):
            code = code[1:]

        # When no magic, return run command
        if not code.startswith("%"):
            code = "%{} {}".format(self.run_command, code)

        # Remove percentage sign
        code = code[1:]

        split_code = code.split(None, 1)
        subcommand = split_code[0].lower()
        flags = []
        if len(split_code) > 1:
            rest = split_code[1]
        else:
            rest = ""

        # Get all flags
        flag_split = rest.split(None, 1)
        while len(flag_split) >= 1 and flag_split[0].startswith("-"):
            if len(flag_split) >= 2:
                flags.append(flag_split[0][1:].lower())
                rest = flag_split[1]
                flag_split = rest.split(None, 1)
            if len(flag_split) == 1:
                flags.append(flag_split[0][1:].lower())
                flag_split = [""]

        # flags to lower
        flags = [i.lower() for i in flags]

        return subcommand, flags, rest

    def _execute_cell(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False,
                      shutdown_if_error=False, log_if_error=None):
        reply_content = self._execute_cell_for_user(code, silent, store_history, user_expressions, allow_stdin)

        if shutdown_if_error and reply_content[u"status"] == u"error":
            error_from_reply = reply_content[u"evalue"]
            if log_if_error is not None:
                message = "{}\nException details:\n\t\"{}\"".format(log_if_error, error_from_reply)
                self._abort_with_fatal_error(message)

        return reply_content

    def _execute_cell_for_user(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        return super(SparkKernelBase, self).do_execute(code, silent, store_history, user_expressions, allow_stdin)

    def _do_shutdown_ipykernel(self, restart):
        return super(SparkKernelBase, self).do_shutdown(restart)

    def _show_user_error(self, message):
        self._logger.error(message)
        self._ipython_display.send_error(message)

    def _queue_fatal_error(self, message):
        """Queues up a fatal error to be thrown when the next cell is executed; does not
        raise an error immediately. We use this for errors that happen on kernel startup,
        since IPython crashes if we throw an exception in the __init__ method."""
        self._fatal_error = message

    def _abort_with_fatal_error(self, message):
        """Queues up a fatal error and throws it immediately."""
        self._queue_fatal_error(message)
        self._repeat_fatal_error()

    def _repeat_fatal_error(self):
        """Throws an error that has already been queued."""
        error = conf.fatal_error_suggestion().format(self._fatal_error)
        self._logger.error(error)
        self._ipython_display.send_error(error)
        raise ValueError(self._fatal_error)