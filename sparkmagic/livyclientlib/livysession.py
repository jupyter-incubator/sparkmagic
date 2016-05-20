# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from time import sleep, time

import hdijupyterutils.configuration as conf
import hdijupyterutils.constants as constants
from hdijupyterutils.guid import ObjectWithGuid
from hdijupyterutils.log import Log
from hdijupyterutils.sparkevents import SparkEvents

from .command import Command
from .exceptions import FailedToCreateSqlContextException, LivyClientTimeoutException, \
    LivyUnexpectedStatusException, BadUserDataException


class LivySession(ObjectWithGuid):
    def __init__(self, http_client, properties, ipython_display,
                 session_id=-1, sql_created=None, spark_events=None):
        super(LivySession, self).__init__()
        assert u"kind" in list(properties.keys())
        kind = properties[u"kind"]
        self.properties = properties
        self.ipython_display = ipython_display

        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events

        status_sleep_seconds = conf.status_sleep_seconds()
        statement_sleep_seconds = conf.statement_sleep_seconds()
        wait_for_idle_timeout_seconds = conf.wait_for_idle_timeout_seconds()

        assert status_sleep_seconds > 0
        assert statement_sleep_seconds > 0
        assert wait_for_idle_timeout_seconds > 0
        if session_id == -1 and sql_created is True:
            raise BadUserDataException(u"Cannot indicate sql state without session id.")

        self.logger = Log(u"LivySession")

        kind = kind.lower()
        if kind not in constants.SESSION_KINDS_SUPPORTED:
            raise BadUserDataException(u"Session of kind '{}' not supported. Session must be of kinds {}."
                                       .format(kind, ", ".join(constants.SESSION_KINDS_SUPPORTED)))

        if session_id == -1:
            self.status = constants.NOT_STARTED_SESSION_STATUS
            sql_created = False
        else:
            self.status = constants.BUSY_SESSION_STATUS

        self._app_id = None
        self._logs = u""
        self._http_client = http_client
        self._status_sleep_seconds = status_sleep_seconds
        self._statement_sleep_seconds = statement_sleep_seconds
        self._wait_for_idle_timeout_seconds = wait_for_idle_timeout_seconds

        self.kind = kind
        self.id = session_id
        self.created_sql_context = sql_created

    def __str__(self):
        return u"Session id: {}\tYARN id: {}\tKind: {}\tState: {}\n\tSpark UI: {}\n\tDriver Log: {}"\
            .format(self.id, self.get_app_id(), self.kind, self.status, self.get_spark_ui_url(), self.get_driver_log_url())

    def start(self, create_sql_context=True):
        """Start the session against actual livy server."""
        self._spark_events.emit_session_creation_start_event(self.guid, self.kind)

        try:
            r = self._http_client.post_session(self.properties)
            self.id = r[u"id"]
            self.status = str(r[u"state"])

            self.ipython_display.writeln(u"Creating SparkContext as 'sc'")
            # We wait for livy_session_startup_timeout_seconds() for the session to start up.
            try:
                self.wait_for_idle(conf.livy_session_startup_timeout_seconds())
            except LivyClientTimeoutException:
                raise LivyClientTimeoutException(u"Session {} did not start up in {} seconds."
                                                 .format(self.id, conf.livy_session_startup_timeout_seconds()))

            if create_sql_context:
                self.create_sql_context()
        except Exception as e:
            self._spark_events.emit_session_creation_end_event(self.guid, self.kind, self.id, self.status,
                                                               False, e.__class__.__name__, str(e))
            raise
        else:
            self._spark_events.emit_session_creation_end_event(self.guid, self.kind, self.id, self.status, True, "", "")

    def create_sql_context(self):
        """Create a sqlContext object on the session. Object will be accessible via variable 'sqlContext'."""
        if self.created_sql_context:
            return
        self.logger.debug(u"Starting '{}' hive session.".format(self.kind))
        self.ipython_display.writeln(u"Creating HiveContext as 'sqlContext'")
        command = self._get_sql_context_creation_command()
        try:
            (success, out) = command.execute(self)
        except LivyClientTimeoutException:
            raise LivyClientTimeoutException(u"Failed to create the SqlContext in time. Timed out after {} seconds."
                                             .format(self._wait_for_idle_timeout_seconds))
        if success:
            self.created_sql_context = True
        else:
            raise FailedToCreateSqlContextException(u"Failed to create the SqlContext.\nError: '{}'".format(out))

    def get_app_id(self):
        if self._app_id is None:
            self._app_id = self._http_client.get_session(self.id).get("appId")
        return self._app_id

    def get_app_info(self):
        appInfo = self._http_client.get_session(self.id).get("appInfo")
        return appInfo if appInfo is not None else {}

    def get_app_info_member(self, member_name):
        return self.get_app_info().get(member_name)

    def get_driver_log_url(self):
        return self.get_app_info_member("driverLogUrl")

    def get_logs(self):
        log_array = self._http_client.get_all_session_logs(self.id)[u'log']
        self._logs = "\n".join(log_array)
        return self._logs

    def get_spark_ui_url(self):
        return self.get_app_info_member("sparkUiUrl")

    @property
    def http_client(self):
        return self._http_client

    @staticmethod
    def is_final_status(status):
        return status in constants.FINAL_STATUS

    def delete(self):
        session_id = self.id
        self._spark_events.emit_session_deletion_start_event(self.guid, self.kind, session_id, self.status)

        try:
            self.logger.debug(u"Deleting session '{}'".format(session_id))

            if self.status != constants.NOT_STARTED_SESSION_STATUS:
                self._http_client.delete_session(session_id)
                self.status = constants.DEAD_SESSION_STATUS
                self.id = -1
            else:
                self.ipython_display.send_error(u"Cannot delete session {} that is in state '{}'."
                                                .format(session_id, self.status))
        except Exception as e:
            self._spark_events.emit_session_deletion_end_event(self.guid, self.kind, session_id, self.status, False,
                                                               e.__class__.__name__, str(e))
            raise
        else:
            self._spark_events.emit_session_deletion_end_event(self.guid, self.kind, session_id, self.status, True, "", "")

    def wait_for_idle(self, seconds_to_wait=None):
        """Wait for session to go to idle status. Sleep meanwhile. Calls done every status_sleep_seconds as
        indicated by the constructor.

        Parameters:
            seconds_to_wait : number of seconds to wait before giving up.
        """
        if seconds_to_wait is None:
            seconds_to_wait = self._wait_for_idle_timeout_seconds

        while True:
            self.refresh_status()
            if self.status == constants.IDLE_SESSION_STATUS:
                return

            if self.status in constants.FINAL_STATUS:
                error = u"Session {} unexpectedly reached final status '{}'."\
                    .format(self.id, self.status)
                self.logger.error(error)
                raise LivyUnexpectedStatusException(u'{} See logs:\n{}'.format(error, self.get_logs()))

            if seconds_to_wait <= 0.0:
                error = u"Session {} did not reach idle status in time. Current status is {}."\
                    .format(self.id, self.status)
                self.logger.error(error)
                raise LivyClientTimeoutException(error)

            start_time = time()
            self.logger.debug(u"Session {} in state {}. Sleeping {} seconds."
                              .format(self.id, self.status, self._status_sleep_seconds))
            sleep(self._status_sleep_seconds)
            seconds_to_wait -= time() - start_time

    def sleep(self):
        sleep(self._statement_sleep_seconds)

    def refresh_status(self):
        status = self._http_client.get_session(self.id)[u'state']

        if status in constants.POSSIBLE_SESSION_STATUS:
            self.status = status
        else:
            raise LivyUnexpectedStatusException(u"Status '{}' not supported by session.".format(status))

        return self.status

    def _get_sql_context_creation_command(self):
        if self.kind == constants.SESSION_KIND_SPARK:
            sql_context_command = u"val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)"
        elif self.kind == constants.SESSION_KIND_PYSPARK:
            sql_context_command = u"from pyspark.sql import HiveContext\nsqlContext = HiveContext(sc)"
        elif self.kind == constants.SESSION_KIND_SPARKR:
            sql_context_command = u"sqlContext <- sparkRHive.init(sc)"
        else:
            raise BadUserDataException(u"Do not know how to create HiveContext in session of kind {}.".format(self.kind))

        return Command(sql_context_command)
