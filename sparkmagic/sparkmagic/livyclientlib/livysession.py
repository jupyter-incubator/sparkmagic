# Distributed under the terms of the Modified BSD License.
import threading
from time import sleep, time

from hdijupyterutils.guid import ObjectWithGuid

import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html
from .configurableretrypolicy import ConfigurableRetryPolicy
from .command import Command
from .exceptions import LivyClientTimeoutException, \
    LivyUnexpectedStatusException, BadUserDataException, SqlContextNotFoundException


class _HeartbeatThread(threading.Thread):
    def __init__(self, livy_session, refresh_seconds, retry_seconds, run_at_most=None):
        """
        Parameters
        ----------
        livy_session : LivySession
        refresh_seconds: int
            The seconds to sleep between refreshing the Livy session status and info
        retry_seconds: int
            The seconds to sleep before retrying on a failed refresh
        run_at_most: int, optional
            The max number of loops to execute before ending this thread
        """
        super(_HeartbeatThread, self).__init__()

        self.livy_session = livy_session
        self.refresh_seconds = refresh_seconds
        self.retry_seconds = retry_seconds
        if run_at_most is None:
            # a billion iterations should be sufficient
            run_at_most = int(1e9)
        self.run_at_most = run_at_most

    def run(self):
        loop_counter = 0
        if self.livy_session is None:
            print(u"Will not start heartbeat thread because self.livy_session is None")
            return

        self.livy_session.logger.info(u'Starting heartbeat for session {}'.format(self.livy_session.id))

        while self.livy_session is not None and loop_counter < self.run_at_most:
            loop_counter += 1

            try:
                sleep_time = self.refresh_seconds
                self.livy_session.refresh_status_and_info()
            except Exception as e:
                sleep_time = self.retry_seconds
                # The built-in python logger has exception handling built in. If you expose
                # the "exception" function in the SparkLog class then you could just make this
                # self.livy_session.logger.exception("some useful message") and it'll print
                # out the stack trace too.
                self.livy_session.logger.error(u'{}'.format(e))

            sleep(sleep_time)


    def stop(self):
        if self.livy_session is not None:
            self.livy_session.logger.info(u'Stopping heartbeat for session {}'.format(self.livy_session.id))

        self.livy_session = None
        self.join()


class LivySession(ObjectWithGuid):
    def __init__(self, http_client, properties, ipython_display,
                 session_id=-1, spark_events=None,
                 heartbeat_timeout=0, heartbeat_thread=None):
        super(LivySession, self).__init__()
        assert constants.LIVY_KIND_PARAM in list(properties.keys())
        kind = properties[constants.LIVY_KIND_PARAM]

        should_heartbeat = False
        if heartbeat_timeout > 0:
            should_heartbeat = True
            properties[constants.LIVY_HEARTBEAT_TIMEOUT_PARAM] = heartbeat_timeout
        elif constants.LIVY_HEARTBEAT_TIMEOUT_PARAM in list(properties.keys()):
            properties.pop(constants.LIVY_HEARTBEAT_TIMEOUT_PARAM)

        self.properties = properties
        self.ipython_display = ipython_display
        self._should_heartbeat = should_heartbeat
        self._user_passed_heartbeat_thread = heartbeat_thread

        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events

        self._policy = ConfigurableRetryPolicy(retry_seconds_to_sleep_list=[0.2, 0.5, 0.5, 1, 1, 2], max_retries=5000)
        wait_for_idle_timeout_seconds = conf.wait_for_idle_timeout_seconds()

        assert wait_for_idle_timeout_seconds > 0

        self.logger = SparkLog(u"LivySession")

        kind = kind.lower()
        if kind not in constants.SESSION_KINDS_SUPPORTED:
            raise BadUserDataException(u"Session of kind '{}' not supported. Session must be of kinds {}."
                                       .format(kind, ", ".join(constants.SESSION_KINDS_SUPPORTED)))

        self._app_id = None
        self._logs = u""
        self._http_client = http_client
        self._wait_for_idle_timeout_seconds = wait_for_idle_timeout_seconds
        self._printed_resource_warning = False

        self.kind = kind
        self.id = session_id
        self.session_info = u""

        self._heartbeat_thread = None
        if session_id == -1:
            self.status = constants.NOT_STARTED_SESSION_STATUS
        else:
            self.status = constants.BUSY_SESSION_STATUS
            self._start_heartbeat_thread()

    def __str__(self):
        return u"Session id: {}\tYARN id: {}\tKind: {}\tState: {}\n\tSpark UI: {}\n\tDriver Log: {}"\
            .format(self.id, self.get_app_id(), self.kind, self.status, self.get_spark_ui_url(), self.get_driver_log_url())

    def start(self):
        """Start the session against actual livy server."""
        self._spark_events.emit_session_creation_start_event(self.guid, self.kind)
        self._printed_resource_warning = False

        try:
            r = self._http_client.post_session(self.properties)
            self.id = r[u"id"]
            self.status = str(r[u"state"])

            self.ipython_display.writeln(u"Starting Spark application")

            # Start heartbeat thread to keep Livy interactive session alive.
            self._start_heartbeat_thread()

            # We wait for livy_session_startup_timeout_seconds() for the session to start up.
            try:
                self.wait_for_idle(conf.livy_session_startup_timeout_seconds())
            except LivyClientTimeoutException:
                raise LivyClientTimeoutException(u"Session {} did not start up in {} seconds."
                                                 .format(self.id, conf.livy_session_startup_timeout_seconds()))

            html = get_sessions_info_html([self], self.id)
            self.ipython_display.html(html)

            command = Command("spark")
            (success, out, mimetype) = command.execute(self)

            if success:
                self.ipython_display.writeln(u"SparkSession available as 'spark'.")
                self.sql_context_variable_name = "spark"
            else:
                command = Command("sqlContext")
                (success, out, mimetype) = command.execute(self)
                if success:
                    self.ipython_display.writeln(u"SparkContext available as 'sc'.")
                    if ("hive" in out.lower()):
                        self.ipython_display.writeln(u"HiveContext available as 'sqlContext'.")
                    else:
                        self.ipython_display.writeln(u"SqlContext available as 'sqlContext'.")
                    self.sql_context_variable_name = "sqlContext"
                else:
                    raise SqlContextNotFoundException(u"Neither SparkSession nor HiveContext/SqlContext is available.")
        except Exception as e:
            self._spark_events.emit_session_creation_end_event(self.guid, self.kind, self.id, self.status,
                                                               False, e.__class__.__name__, str(e))
            raise
        else:
            self._spark_events.emit_session_creation_end_event(self.guid, self.kind, self.id, self.status, True, "", "")

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

    @property
    def endpoint(self):
        return self._http_client.endpoint

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
                self._stop_heartbeat_thread()
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
        """Wait for session to go to idle status. Sleep meanwhile.

        Parameters:
            seconds_to_wait : number of seconds to wait before giving up.
        """
        if seconds_to_wait is None:
            seconds_to_wait = self._wait_for_idle_timeout_seconds

        retries = 1
        while True:
            self.refresh_status_and_info()
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

            if constants.YARN_RESOURCE_LIMIT_MSG in self.session_info and \
                not self._printed_resource_warning:
                self.ipython_display.send_error(constants.RESOURCE_LIMIT_WARNING\
                                                .format(conf.resource_limit_mitigation_suggestion()))
                self._printed_resource_warning = True

            start_time = time()
            sleep_time = self._policy.seconds_to_sleep(retries)
            retries += 1

            self.logger.debug(u"Session {} in state {}. Sleeping {} seconds."
                              .format(self.id, self.status, sleep_time))
            sleep(sleep_time)
            seconds_to_wait -= time() - start_time

    def sleep(self, retries):
        sleep(self._policy.seconds_to_sleep(retries))

    # This function will refresh the status and get the logs in a single call.
    # Only the status will be returned as the return value.
    def refresh_status_and_info(self):
        response = self._http_client.get_session(self.id)
        status = response[u'state']
        log_array = response[u'log']

        if status in constants.POSSIBLE_SESSION_STATUS:
            self.status = status
            self.session_info = u"\n".join(log_array)
        else:
           raise LivyUnexpectedStatusException(u"Status '{}' not supported by session.".format(status))

    def _start_heartbeat_thread(self):
        if self._should_heartbeat and self._heartbeat_thread is None:
            refresh_seconds = conf.heartbeat_refresh_seconds()
            retry_seconds = conf.heartbeat_retry_seconds()

            if self._user_passed_heartbeat_thread is None:
                self._heartbeat_thread = _HeartbeatThread(self, refresh_seconds, retry_seconds)
            else:
                self._heartbeat_thread = self._user_passed_heartbeat_thread

            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self):
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.stop()
            self._heartbeat_thread = None

    def get_row_html(self, current_session_id):
        return u"""<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td><td>{6}</td></tr>""".format(
            self.id, self.get_app_id(), self.kind, self.status,
            self.get_html_link(u'Link', self.get_spark_ui_url()), self.get_html_link(u'Link', self.get_driver_log_url()),
            u"" if current_session_id is None or current_session_id != self.id else u"\u2714"
        )

    @staticmethod
    def get_html_link(text, url):
        if url is not None:
            return u"""<a target="_blank" href="{1}">{0}</a>""".format(text, url)
        else:
            return u""
