# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from ipykernel.ipkernel import IPythonKernel
import requests

from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.utils import get_connection_string
from remotespark.livyclientlib.configuration import get_configuration
from remotespark.livyclientlib.constants import Constants


class SparkKernelBase(IPythonKernel):
    fatal_error_suggestion = "The code failed because of a fatal error:\n\t{}.\n\nSome things to try:\n" \
                             "a) Make sure Spark has enough available resources for Jupyter to create a Spark context."\
                             "\nb) Contact your Jupyter administrator to make sure the Spark magics library is " \
                             "configured correctly." \
                             "\nc) Restart the kernel."

    # Required by Jupyter - Override
    implementation = None
    implementation_version = None
    language = None
    language_version = None
    language_info = None
    banner = None

    # Override
    username_conf_name = None
    password_conf_name = None
    url_conf_name = None
    session_language = None
    client_name = None

    def __init__(self, **kwargs):
        super(SparkKernelBase, self).__init__(**kwargs)
        self.logger = Log(self.client_name)
        self.already_ran_once = False
        self._fatal_error = None

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if self._fatal_error is not None:
            self._abort_with_fatal_error(self._fatal_error)

        if not self.already_ran_once:
            (username, password, url) = self._get_configuration()
            self._initialize_magics(username, password, url)

        # Modify code by prepending spark magic text
        if code.lower().startswith("%sql\n") or code.lower().startswith("%sql "):
            code = "%%spark -c sql\n{}".format(code[5:])
        elif code.lower().startswith("%%sql\n") or code.lower().startswith("%%sql "):
            code = "%%spark -c sql\n{}".format(code[6:])
        elif code.lower().startswith("%hive\n") or code.lower().startswith("%hive "):
            code = "%%spark -c hive\n{}".format(code[6:])
        elif code.lower().startswith("%%hive\n") or code.lower().startswith("%%hive "):
            code = "%%spark -c hive\n{}".format(code[7:])
        else:
            code = "%%spark\n{}".format(code)

        return self._execute_cell(code, silent, store_history, user_expressions, allow_stdin)

    def do_shutdown(self, restart):
        # Cleanup
        if self.already_ran_once:
            code = "%spark cleanup"
            self._execute_cell_for_user(code, True, False)
            self.already_ran_once = False

        return self._do_shutdown_ipykernel(restart)

    def _initialize_magics(self, username, password, url):
        connection_string = get_connection_string(url, username, password)

        register_magics_code = "%load_ext remotespark"
        self._execute_cell(register_magics_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to load the Spark magics library.")
        self.logger.debug("Loaded magics.")

        self.already_ran_once = True

        add_endpoint_code = "%spark add {} {} {} skip".format(
            self.client_name, self.session_language, connection_string)
        self._execute_cell(add_endpoint_code, True, False, shutdown_if_error=True,
                           log_if_error="Failed to create a Livy session.")
        self.logger.debug("Added endpoint.")

    def _get_configuration(self):
        try:
            username = get_configuration(self.username_conf_name)
            password = get_configuration(self.password_conf_name)
            url = get_configuration(self.url_conf_name)
            return username, password, url
        except KeyError:
            message = "Please set configuration for '{}', '{}', '{} to initialize Kernel.".format(
                self.username_conf_name, self.password_conf_name, self.url_conf_name)
            self._abort_with_fatal_error(message)

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

    def _abort_with_fatal_error(self, message):
        self._fatal_error = message

        error = get_configuration(Constants.fatal_error_suggestion, self.fatal_error_suggestion).format(message)
        self.logger.error(error)
        self._send_error(error)

        raise ValueError(message)

    def _send_error(self, error):
        stream_content = {"name": "stderr", "text": error}
        self._ipython_send_error(stream_content)

    def _ipython_send_error(self, stream_content):
        self.send_response(self.iopub_socket, "stream", stream_content)
