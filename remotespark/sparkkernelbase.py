# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from ipykernel.ipkernel import IPythonKernel
import altair.api as alt
import pandas as pd
import requests

from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.utils import get_connection_string, read_environment_variable


class SparkKernelBase(IPythonKernel):
    # Required by Jupyter - Override
    implementation = None
    implementation_version = None
    language = None
    language_version = None
    language_info = None
    banner = None

    # Override
    username_env_var = None
    password_env_var = None
    url_env_var = None
    session_language = None
    client_name = None

    def __init__(self, **kwargs):
        super(SparkKernelBase, self).__init__(**kwargs)
        self.logger = Log(self.client_name)
        self.already_ran_once = False

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

        try:
            # Try to initialize altair
            # from lightning.visualization import VisualizationLocal
            # from IPython.core.getipython import get_ipython
            # from IPython.display import display, Javascript, HTML
            # ip = get_ipython()
            # formatter = ip.display_formatter.formatters['text/html']
            # js = VisualizationLocal.load_embed()
            # display(HTML("<script>" + js + "</script>"))
            # formatter.for_type(VisualizationLocal, lambda viz, kwds=None: viz.get_html())

            # Use lightning here so that the graphic doesn't display later when magics are registered
            # alt.use_renderer('lightning')

            # Create a dumb viz so that comparison warning is not shown
            dummy_records = [{u'date': u'6/1/13', u'temp_diff': 8, u'buildingID': u'4'}]
            dummy_df = pd.DataFrame(dummy_records)
            alt.Viz(dummy_df)
        except:
            self.logger.error("Could not initialize renderer or create dummy records")

    def get_configuration(self):
        try:
            username = read_environment_variable(self.username_env_var)
            password = read_environment_variable(self.password_env_var)
            url = read_environment_variable(self.url_env_var)
            return username, password, url
        except KeyError:
            error = "FATAL ERROR: Please set environment variables '{}', '{}', '{} to initialize Kernel.".format(
                self.username_env_var, self.password_env_var, self.url_env_var)
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

    def execute_cell_for_user(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        return super(SparkKernelBase, self).do_execute(code, silent, store_history, user_expressions, allow_stdin)

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if not self.already_ran_once:
            (username, password, url) = self.get_configuration()
            self.initialize_magics(username, password, url)

        # Modify code by prepending spark magic text
        if code.lower().startswith("%sql\n") or code.lower().startswith("%sql "):
            code = "%%spark -s True\n{}".format(code[5:])
        elif code.lower().startswith("%%sql\n") or code.lower().startswith("%%sql "):
            code = "%%spark -s True\n{}".format(code[6:])
        else:
            code = "%%spark\n{}".format(code)

        return self.execute_cell_for_user(code, silent, store_history, user_expressions, allow_stdin)

    def do_shutdown(self, restart):
        # Cleanup
        if self.already_ran_once:
            code = "%spark cleanup"
            self.execute_cell_for_user(code, True, False)
            self.already_ran_once = False

        return super(SparkKernelBase, self).do_shutdown(restart)
