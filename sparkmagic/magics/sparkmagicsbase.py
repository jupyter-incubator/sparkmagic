# -*- coding: UTF-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay
from hdijupyterutils.log import Log
from hdijupyterutils.sparkevents import SparkEvents

from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery


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

    def _print_endpoint_info(self, info_sessions, current_session_id):
        info_sessions = sorted(info_sessions, key=lambda s: s.id)
        html = u"""<table>
<tr><th>ID</th><th>YARN application ID</th><th>Kind</th><th>State</th><th>Spark UI link</th><th>Driver log</th><th>Current session?</th></tr>""" + \
            u"".join([SparkMagicBase._session_row_html(session, current_session_id) for session in info_sessions]) + \
            u"</table>"
        self.ipython_display.html(html)

    @staticmethod
    def _session_row_html(session, current_session_id):
        return u"""<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td><td>{6}</td></tr>""".format(
            session.id, session.get_app_id(), session.kind, session.status,
            SparkMagicBase._link(session.get_spark_ui_url()), SparkMagicBase._link(session.get_driver_log_url()),
            u"" if current_session_id is None or current_session_id != session.id else u"✔"
        )

    @staticmethod
    def _link(url):
        if url is not None:
            return u"""<a href="{0}">{0}</a>""".format(url)
        else:
            return u""
