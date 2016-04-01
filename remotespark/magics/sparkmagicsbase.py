"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class

from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.log import Log
from remotespark.utils.sparkevents import SparkEvents
from remotespark.livyclientlib.sparkcontroller import SparkController
from remotespark.livyclientlib.sqlquery import SQLQuery


@magics_class
class SparkMagicBase(Magics):
    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = Log("SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)

        self.logger.debug("Initialized spark magics.")

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         session, output_var, quiet):
        sqlquery = self._sqlquery(cell, samplemethod, maxrows, samplefraction)
        df = self.spark_controller.run_sqlquery(sqlquery, session)
        if output_var is not None:
            self.shell.user_ns[output_var] = df
        if quiet:
            return None
        else:
            return df

    @staticmethod
    def _sqlquery(cell, samplemethod, maxrows, samplefraction):
        return SQLQuery(cell, samplemethod, maxrows, samplefraction)

    @staticmethod
    def print_endpoint_info(info_sessions):
        sessions_info = ["        {}".format(i) for i in info_sessions]
        print("""Info for endpoint:
    Sessions:
{}
""".format("\n".join(sessions_info)))
