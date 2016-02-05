"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class

import remotespark.utils.configuration as conf
from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.log import Log
from remotespark.utils.utils import get_magics_home_path, join_paths
from remotespark.livyclientlib.sparkcontroller import SparkController
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException


@magics_class
class SparkMagicBase(Magics):
    def __init__(self, shell, data=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = Log("SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        try:
            should_serialize = conf.serialize()
            if should_serialize:
                self.logger.debug("Serialization enabled.")

                self.magics_home_path = get_magics_home_path()
                path_to_serialize = join_paths(self.magics_home_path, "state.json")

                self.logger.debug("Will serialize to {}.".format(path_to_serialize))

                self.spark_controller = SparkController(self.ipython_display, serialize_path=path_to_serialize)
            else:
                self.logger.debug("Serialization NOT enabled.")
        except KeyError:
            self.logger.error("Could not read env vars for serialization.")

        self.logger.debug("Initialized spark magics.")

    def execute_against_context_that_returns_df(self, method, cell, session, output_var):
        try:
            df = method(cell, session)
            if output_var is not None:
                self.shell.user_ns[output_var] = df
            return df
        except DataFrameParseException as e:
            self.ipython_display.send_error(e.out)
            return None

    @staticmethod
    def print_endpoint_info(info_sessions):
        sessions_info = ["        {}".format(i) for i in info_sessions]
        print("""Info for endpoint:
    Sessions:
{}
""".format("\n".join(sessions_info)))
