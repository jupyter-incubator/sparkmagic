# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from ipykernel.ipkernel import IPythonKernel
import requests

from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.utils import get_connection_string
from remotespark.livyclientlib.configuration import get_configuration


class SparkKernelBase(IPythonKernel):
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

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

    def get_configuration(self):
        try:
            username = get_configuration(self.username_conf_name)
            password = get_configuration(self.password_conf_name)
            url = get_configuration(self.url_conf_name)
            return username, password, url
        except KeyError:
            error = "FATAL ERROR: Please set configuration for '{}', '{}', '{} to initialize Kernel.".format(
                self.username_conf_name, self.password_conf_name, self.url_conf_name)
            stream_content = {"name": "stdout", "text": error}
            self.send_response(self.iopub_socket, "stream", stream_content)
            self.logger.error(error)

            self.do_shutdown(False)

            # Not reachable because shutdown will kill the kernel
            raise

    def initialize_magics(self, username, password, url):
        connection_string = get_connection_string(url, username, password)

        register_magics_code = "%load_ext remotespark"
        self.execute_cell_for_user(register_magics_code, True, False)
        self.logger.debug("Loaded magics.")

        add_endpoint_code = "%spark add {} {} {} skip".format(
            self.client_name, self.session_language, connection_string)
        self.execute_cell_for_user(add_endpoint_code, True, False)
        self.logger.debug("Added endpoint.")

        self.already_ran_once = True

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if not self.already_ran_once:
            (username, password, url) = self.get_configuration()
            self.initialize_magics(username, password, url)

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

        return self.execute_cell_for_user(code, silent, store_history, user_expressions, allow_stdin)

    def do_shutdown(self, restart):
        # Cleanup
        if self.already_ran_once:
            code = "%spark cleanup"
            self.execute_cell_for_user(code, True, False)
            self.already_ran_once = False

        return self.do_shutdown_ipykernel(restart)

    def execute_cell_for_user(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        return super(SparkKernelBase, self).do_execute(code, silent, store_history, user_expressions, allow_stdin)

    def do_shutdown_ipykernel(self, restart):
        return super(SparkKernelBase, self).do_shutdown(restart)
