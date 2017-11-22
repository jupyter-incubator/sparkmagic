import json
from mock import MagicMock, call
from nose.tools import raises, assert_equals

import sparkmagic.utils.constants as constants
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import LivyClientTimeoutException, LivyUnexpectedStatusException,\
    BadUserDataException, SqlContextNotFoundException
from sparkmagic.livyclientlib.livysession import LivySession


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text

    def json(self):
        return json.loads(self._json_text)

    @property
    def status_code(self):
        return self._status_code


class TestLivySession(object):
    _multiprocess_can_split_ = True

    pi_result = "Pi is roughly 3.14336"

    session_create_json = json.loads('{"id":0,"state":"starting","kind":"spark","log":[]}')
    resource_limit_json = json.loads('{"id":0,"state":"starting","kind":"spark","log":['
                                     '"Queue\'s AM resource limit exceeded."]}')
    ready_sessions_json = json.loads('{"id":0,"state":"idle","kind":"spark","log":[""]}')
    error_sessions_json = json.loads('{"id":0,"state":"error","kind":"spark","log":[""]}')
    busy_sessions_json = json.loads('{"id":0,"state":"busy","kind":"spark","log":[""]}')
    post_statement_json = json.loads('{"id":0,"state":"running","output":null}')
    waiting_statement_json = json.loads('{"id":0,"state":"waiting","output":null}')
    running_statement_json = json.loads('{"id":0,"state":"running","output":null}')
    ready_statement_json = json.loads('{"id":0,"state":"available","output":{"status":"ok",'
                                      '"execution_count":0,"data":{"text/plain":"Pi is roughly 3.14336"}}}')
    ready_statement_null_output_json = json.loads('{"id":0,"state":"available","output":null}')
    ready_statement_failed_json = json.loads('{"id":0,"state":"available","output":{"status":"error",'
                                             '"evalue":"error","traceback":"error"}}')
    log_json = json.loads('{"id":6,"from":0,"total":212,"log":["hi","hi"]}')

    def __init__(self):
        self.http_client = None
        self.spark_events = None
        
        self.get_statement_responses = []
        self.post_statement_responses = []
        self.get_session_responses = []
        self.post_session_responses = []

    def setup(self):
        self.http_client = MagicMock()
        self.spark_events = MagicMock()
        self.heartbeat_thread = MagicMock()

    def _next_statement_response_get(self, *args):
        val = self.get_statement_responses[0]
        self.get_statement_responses = self.get_statement_responses[1:]
        return val

    def _next_statement_response_post(self, *args):
        val = self.post_statement_responses[0]
        self.post_statement_responses = self.post_statement_responses[1:]
        return val

    def _next_session_response_get(self, *args):
        val = self.get_session_responses[0]
        self.get_session_responses = self.get_session_responses[1:]
        return val

    def _next_session_response_post(self, *args):
        val = self.post_session_responses[0]
        self.post_session_responses = self.post_session_responses[1:]
        return val

    def _create_session(self, kind=constants.SESSION_KIND_SPARK, session_id=-1,
                        heartbeat_timeout=60):
        ipython_display = MagicMock()
        session = LivySession(self.http_client,
                              {"kind": kind},
                              ipython_display,
                              session_id,
                              self.spark_events,
                              heartbeat_timeout,
                              self.heartbeat_thread)
        return session

    def _create_session_with_fixed_get_response(self, get_session_json):
        self.http_client.get_session.return_value = get_session_json
        self.http_client.get_statement.return_value = self.ready_statement_json
        session = self._create_session()
        session.start()
        return session

    def test_doesnt_do_anything_or_create_sql_context_automatically(self):
        # If the session object does anything (attempts to create a session or run
        # a statement), the http_client will fail
        self.http_client = MagicMock(side_effect=ValueError)
        self._create_session()

    def test_constructor_starts_with_existing_session(self):
        session_id = 1
        session = self._create_session(session_id=session_id, heartbeat_timeout=0)

        assert session.id == session_id
        assert session._heartbeat_thread is None
        assert constants.LIVY_HEARTBEAT_TIMEOUT_PARAM not in list(session.properties.keys())
        
    def test_constructor_starts_heartbeat_with_existing_session(self):
        conf.override_all({
            "heartbeat_refresh_seconds": 0.1
        })
        session_id = 1
        session = self._create_session(session_id=session_id)
        conf.override_all({})
        
        assert session.id == session_id
        assert self.heartbeat_thread.daemon
        self.heartbeat_thread.start.assert_called_once_with()
        assert not session._heartbeat_thread is None
        assert session.properties[constants.LIVY_HEARTBEAT_TIMEOUT_PARAM ] > 0
        
    def test_start_with_heartbeat(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        
        assert self.heartbeat_thread.daemon
        self.heartbeat_thread.start.assert_called_once_with()
        assert not session._heartbeat_thread is None
        assert session.properties[constants.LIVY_HEARTBEAT_TIMEOUT_PARAM ] > 0
        
    def test_start_with_heartbeat_calls_only_once(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        session.start()
        session.start()

        assert self.heartbeat_thread.daemon
        self.heartbeat_thread.start.assert_called_once_with()
        assert not session._heartbeat_thread is None
        
    def test_delete_with_heartbeat(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        heartbeat_thread = session._heartbeat_thread
        
        session.delete()
        
        self.heartbeat_thread.stop.assert_called_once_with()
        assert session._heartbeat_thread is None

    def test_constructor_starts_with_no_session(self):
        session = self._create_session()

        assert session.id == -1

    def test_is_final_status(self):
        session = self._create_session()

        assert not session.is_final_status("idle")
        assert not session.is_final_status("starting")
        assert not session.is_final_status("busy")

        assert session.is_final_status("dead")
        assert session.is_final_status("error")

    def test_start_scala_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.start()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "spark", "heartbeatTimeoutInSecond": 60})

    def test_start_r_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_SPARKR
        session = self._create_session(kind=kind)
        session.start()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "sparkr", "heartbeatTimeoutInSecond": 60})

    def test_start_python_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_PYSPARK
        session = self._create_session(kind=kind)
        session.start()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "pyspark", "heartbeatTimeoutInSecond": 60, "conf": {"spark.pyspark.python": "python"}})

    def test_start_python3_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_PYSPARK3
        session = self._create_session(kind=kind)
        session.start()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "pyspark", "heartbeatTimeoutInSecond": 60, "conf": {"spark.pyspark.python": "python3"}})

    def test_start_passes_in_all_properties(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_SPARK
        properties = {"kind": kind, "extra": 1}

        ipython_display = MagicMock()
        session = LivySession(self.http_client, properties, ipython_display)
        session.start()

        self.http_client.post_session.assert_called_with(properties)

    def test_status_gets_latest_status(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json
        session = self._create_session()
        session.start()

        session.refresh_status_and_info()
        state = session.status

        assert_equals("idle", state)
        self.http_client.get_session.assert_called_with(0)

    def test_logs_gets_latest_logs(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_all_session_logs.return_value = self.log_json
        self.http_client.get_statement.return_value = self.ready_statement_json
        session = self._create_session()
        session.start()

        logs = session.get_logs()

        assert_equals("hi\nhi", logs)
        self.http_client.get_all_session_logs.assert_called_with(0)

    def test_wait_for_idle_returns_when_in_state(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.get_row_html = MagicMock()
        session.get_row_html.return_value = u"""<tr><td>row1</td></tr>"""

        session.start()

        session.wait_for_idle(30)

        self.http_client.get_session.assert_called_with(0)
        assert_equals(4, self.http_client.get_session.call_count)

    def test_wait_for_idle_prints_resource_limit_message(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.resource_limit_json,
                                      self.ready_sessions_json,
                                      self.ready_sessions_json,
                                      self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.http_client.get_statement.return_value = self.ready_statement_json
        self.http_client.get_all_session_logs.return_value = self.log_json

        session = self._create_session()
        session.get_row_html = MagicMock()
        session.get_row_html.return_value = u"""<tr><td>row1</td></tr>"""

        session.start()

        session.wait_for_idle(30)
        assert_equals(session.ipython_display.send_error.call_count, 1)

    @raises(LivyUnexpectedStatusException)
    def test_wait_for_idle_throws_when_in_final_status(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.error_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.http_client.get_all_session_logs.return_value = self.log_json

        session = self._create_session()
        session.get_row_html = MagicMock()
        session.get_row_html.return_value = u"""<tr><td>row1</td></tr>"""

        session.start()

        session.wait_for_idle(30)

    @raises(LivyClientTimeoutException)
    def test_wait_for_idle_times_out(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.get_row_html = MagicMock()
        session.get_row_html.return_value = u"""<tr><td>row1</td></tr>"""

        session.start()

        session.wait_for_idle(0.01)

    def test_delete_session_when_active(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json
        session = self._create_session()
        session.start()

        session.delete()

        assert_equals("dead", session.status)

    def test_delete_session_when_not_started(self):
        self.http_client.post_session.return_value = self.session_create_json
        session = self._create_session()

        session.delete()

        assert_equals(session.ipython_display.send_error.call_count, 1)

    def test_delete_session_when_dead_throws(self):
        self.http_client.post.return_value = self.session_create_json
        session = self._create_session()
        session.status = "dead"

        session.delete()

        assert_equals(session.ipython_display.send_error.call_count, 0)

    def test_start_emits_start_end_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.start()

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, True, "", "")

    def test_start_emits_start_end_failed_session_when_bad_status(self):
        self.http_client.post_session.side_effect = ValueError
        self.http_client.get_session.return_value = self.ready_sessions_json

        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, False, "ValueError", "")

    def test_start_emits_start_end_failed_session_when_wait_for_idle_throws(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.wait_for_idle = MagicMock(side_effect=ValueError)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, False, "ValueError", "")

    def test_delete_session_emits_start_end(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        end_id = session.id
        end_status = constants.BUSY_SESSION_STATUS
        session.status = end_status

        session.delete()

        assert_equals(session.id, -1)
        self.spark_events.emit_session_deletion_start_event.assert_called_once_with(
            session.guid, session.kind, end_id, end_status)
        self.spark_events.emit_session_deletion_end_event.assert_called_once_with(
            session.guid, session.kind, end_id, constants.DEAD_SESSION_STATUS, True, "", "")

    def test_delete_session_emits_start_failed_end_when_delete_throws(self):
        self.http_client.delete_session.side_effect = ValueError
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        session.id = 0
        end_id = session.id
        end_status = constants.BUSY_SESSION_STATUS
        session.status = end_status

        try:
            session.delete()
            assert False
        except ValueError:
            pass

        self.spark_events.emit_session_deletion_start_event.assert_called_once_with(
            session.guid, session.kind, end_id, end_status)
        self.spark_events.emit_session_deletion_end_event.assert_called_once_with(
            session.guid, session.kind, end_id, end_status, False, "ValueError", "")

    def test_delete_session_emits_start_failed_end_when_in_bad_state(self):
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json

        session = self._create_session()
        session.start()
        session.id = 0
        end_id = session.id
        end_status = constants.DEAD_SESSION_STATUS
        session.status = end_status

        session.delete()

        assert_equals(0, session.ipython_display.send_error.call_count)
        self.spark_events.emit_session_deletion_start_event.assert_called_once_with(
            session.guid, session.kind, end_id, end_status)
        self.spark_events.emit_session_deletion_end_event.assert_called_once_with(
            session.guid, session.kind, end_id, constants.DEAD_SESSION_STATUS, True, "", "")

    def test_get_empty_app_id(self):
        self._verify_get_app_id("null", None, 6)

    def test_get_missing_app_id(self):
        self._verify_get_app_id(None, None, 6)

    def test_get_normal_app_id(self):
        self._verify_get_app_id("\"app_id_123\"", "app_id_123", 5)

    def test_get_empty_driver_log_url(self):
        self._verify_get_driver_log_url("null", None)

    def test_get_normal_driver_log_url(self):
        self._verify_get_driver_log_url("\"http://example.com\"", "http://example.com")

    def test_missing_app_info_get_driver_log_url(self):
        self._verify_get_driver_log_url_json(self.ready_sessions_json, None)
        
    def _verify_get_app_id(self, mock_app_id, expected_app_id, expected_call_count):
        mock_field = ",\"appId\":" + mock_app_id if mock_app_id is not None else ""
        get_session_json = json.loads('{"id":0,"state":"idle","output":null%s,"log":""}' % mock_field)
        session = self._create_session_with_fixed_get_response(get_session_json)

        app_id = session.get_app_id()

        assert_equals(expected_app_id, app_id)
        assert_equals(expected_call_count, self.http_client.get_session.call_count)

    def _verify_get_driver_log_url(self, mock_driver_log_url, expected_url):
        mock_field = "\"driverLogUrl\":" + mock_driver_log_url if mock_driver_log_url is not None else ""
        session_json = json.loads('{"id":0,"state":"idle","output":null,"appInfo":{%s},"log":""}' % mock_field)
        self._verify_get_driver_log_url_json(session_json, expected_url)

    def _verify_get_driver_log_url_json(self, get_session_json, expected_url):
        session = self._create_session_with_fixed_get_response(get_session_json)

        driver_log_url = session.get_driver_log_url()

        assert_equals(expected_url, driver_log_url)
        assert_equals(6, self.http_client.get_session.call_count)

    def test_get_empty_spark_ui_url(self):
        self._verify_get_spark_ui_url("null", None)

    def test_get_normal_spark_ui_url(self):
        self._verify_get_spark_ui_url("\"http://example.com\"", "http://example.com")

    def test_missing_app_info_get_spark_ui_url(self):
        self._verify_get_spark_ui_url_json(self.ready_sessions_json, None)

    def _verify_get_spark_ui_url(self, mock_spark_ui_url, expected_url):
        mock_field = "\"sparkUiUrl\":" + mock_spark_ui_url if mock_spark_ui_url is not None else ""
        session_json = json.loads('{"id":0,"state":"idle","output":null,"appInfo":{%s},"log":""}' % mock_field)
        self._verify_get_spark_ui_url_json(session_json, expected_url)

    def _verify_get_spark_ui_url_json(self, get_session_json, expected_url):
        session = self._create_session_with_fixed_get_response(get_session_json)

        spark_ui_url = session.get_spark_ui_url()

        assert_equals(expected_url, spark_ui_url)
        assert_equals(6, self.http_client.get_session.call_count)

    def test_get_row_html(self):
        session_id1 = 1
        session1 = self._create_session(session_id=session_id1)
        session1.get_app_id = MagicMock()
        session1.get_spark_ui_url = MagicMock()
        session1.get_driver_log_url = MagicMock()
        session1.get_app_id.return_value = 'app1234'
        session1.status = constants.IDLE_SESSION_STATUS
        session1.get_spark_ui_url.return_value = 'https://microsoft.com/sparkui'
        session1.get_driver_log_url.return_value = 'https://microsoft.com/driverlog'

        html1 = session1.get_row_html(1)

        assert_equals(html1, u"""<tr><td>1</td><td>app1234</td><td>spark</td><td>idle</td><td><a target="_blank" href="https://microsoft.com/sparkui">Link</a></td><td><a target="_blank" href="https://microsoft.com/driverlog">Link</a></td><td>\u2714</td></tr>""")

        session_id2 = 3
        session2 = self._create_session(kind=constants.SESSION_KIND_PYSPARK,
                                        session_id=session_id2)
        session2.get_app_id = MagicMock()
        session2.get_spark_ui_url = MagicMock()
        session2.get_driver_log_url = MagicMock()
        session2.get_app_id.return_value = 'app5069'
        session2.status = constants.BUSY_SESSION_STATUS
        session2.get_spark_ui_url.return_value = None
        session2.get_driver_log_url.return_value = None

        html2 = session2.get_row_html(1)

        assert_equals(html2, u"""<tr><td>3</td><td>app5069</td><td>pyspark</td><td>busy</td><td></td><td></td><td></td></tr>""")

    def test_link(self):
        url = u"https://microsoft.com"
        assert_equals(LivySession.get_html_link(u'Link', url), u"""<a target="_blank" href="https://microsoft.com">Link</a>""")

        url = None
        assert_equals(LivySession.get_html_link(u'Link', url), u"")

    def test_spark_session_available(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_json
        session = self._create_session()
        session.start()
        assert_equals(session.sql_context_variable_name,"spark")

    def test_sql_context_available(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.ready_statement_failed_json,
                                        self.ready_statement_json]
        self.http_client.get_statement.side_effect = self._next_statement_response_get
        session = self._create_session()
        session.start()
        assert_equals(session.sql_context_variable_name, "sqlContext")

    @raises(SqlContextNotFoundException)
    def test_spark_session_and_sql_context_unavailable(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_statement.return_value = self.ready_statement_failed_json
        session = self._create_session()
        session.start()
