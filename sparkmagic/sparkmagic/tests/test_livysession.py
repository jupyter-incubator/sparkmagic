import json
from mock import MagicMock, call
from nose.tools import raises, assert_equals

import sparkmagic.utils.constants as constants
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import LivyClientTimeoutException, LivyUnexpectedStatusException,\
    BadUserDataException, FailedToCreateSqlContextException
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
    ready_sessions_json = json.loads('{"id":0,"state":"idle","kind":"spark","log":[""]}')
    error_sessions_json = json.loads('{"id":0,"state":"error","kind":"spark","log":[""]}')
    busy_sessions_json = json.loads('{"id":0,"state":"busy","kind":"spark","log":[""]}')
    post_statement_json = json.loads('{"id":0,"state":"running","output":null}')
    running_statement_json = json.loads('{"id":0,"state":"running","output":null}')
    ready_statement_json = json.loads('{"id":0,"state":"available","output":{"status":"ok",'
                                      '"execution_count":0,"data":{"text/plain":"Pi is roughly 3.14336"}}}')
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

    def _create_session(self, kind=constants.SESSION_KIND_SPARK, session_id=-1, sql_created=False):
        ipython_display = MagicMock()
        session = LivySession(self.http_client, {"kind": kind}, ipython_display, session_id, sql_created, self.spark_events)
        return session

    def _create_session_with_fixed_get_response(self, get_session_json):
        self.http_client.get_session.return_value = get_session_json
        session = self._create_session()
        session.start(create_sql_context=False)
        return session

    @raises(AssertionError)
    def test_constructor_throws_status_sleep_seconds(self):
        conf.override_all({
            "status_sleep_seconds": 0,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session()
        conf.override_all({})

    @raises(AssertionError)
    def test_constructor_throws_statement_sleep_seconds(self):
        conf.override_all({
            "status_sleep_seconds": 3,
            "statement_sleep_seconds": 0,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session()
        conf.override_all({})

    @raises(BadUserDataException)
    def test_constructor_throws_invalid_session_sql_combo(self):
        conf.override_all({
            "status_sleep_seconds": 2,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session(sql_created=True)
        conf.override_all({})

    def test_doesnt_do_anything_or_create_sql_context_automatically(self):
        # If the session object does anything (attempts to create a session or run
        # a statement), the http_client will fail
        self.http_client = MagicMock(side_effect=ValueError)
        self._create_session()

    def test_constructor_starts_with_existing_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session_id = 1
        session = self._create_session(session_id=session_id, sql_created=True)
        conf.override_all({})

        assert session.id == session_id
        assert session.created_sql_context

    def test_constructor_starts_with_no_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session = self._create_session()
        conf.override_all({})

        assert session.id == -1
        assert not session.created_sql_context

    def test_is_final_status(self):
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})

        assert not session.is_final_status("idle")
        assert not session.is_final_status("starting")
        assert not session.is_final_status("busy")

        assert session.is_final_status("dead")
        assert session.is_final_status("error")

    def test_start_scala_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()
        session.start()
        conf.override_all({})

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "spark"})
        session.create_sql_context.assert_called_once_with()

    def test_start_r_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARKR
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()
        session.start()
        conf.override_all({})

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "sparkr"})
        session.create_sql_context.assert_called_once_with()

    def test_start_python_starts_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_PYSPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()
        session.start()
        conf.override_all({})

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "pyspark"})
        session.create_sql_context.assert_called_once_with()

    def test_start_turn_off_sql_context_creation(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_PYSPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock(side_effect=ValueError)
        session.start(create_sql_context=False)
        conf.override_all({})

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        self.http_client.post_session.assert_called_with({"kind": "pyspark"})
        session.create_sql_context.assert_not_called()

    def test_start_passes_in_all_properties(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        properties = {"kind": kind, "extra": 1}

        ipython_display = MagicMock()
        session = LivySession(self.http_client, properties, ipython_display)
        session.start(create_sql_context=False)
        conf.override_all({})

        self.http_client.post_session.assert_called_with(properties)

    def test_status_gets_latest_status(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)

        session.refresh_status()
        state = session.status

        assert_equals("idle", state)
        self.http_client.get_session.assert_called_with(0)

    def test_logs_gets_latest_logs(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.http_client.get_all_session_logs.return_value = self.log_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)

        logs = session.get_logs()

        assert_equals("hi\nhi", logs)
        self.http_client.get_all_session_logs.assert_called_with(0)

    def test_wait_for_idle_returns_when_in_state(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})

        session.start(create_sql_context=False)

        session.wait_for_idle(30)

        self.http_client.get_session.assert_called_with(0)
        assert_equals(3, self.http_client.get_session.call_count)

    @raises(LivyUnexpectedStatusException)
    def test_wait_for_idle_throws_when_in_final_status(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.error_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.http_client.get_all_session_logs.return_value = self.log_json

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session()
        conf.override_all({})

        session.start(create_sql_context=False)

        session.wait_for_idle(30)

    @raises(LivyClientTimeoutException)
    def test_wait_for_idle_times_out(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session()
        conf.override_all({})

        session.start(create_sql_context=False)

        session.wait_for_idle(0.01)

    def test_delete_session_when_active(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)

        session.delete()

        assert_equals("dead", session.status)

    def test_delete_session_when_not_started(self):
        self.http_client.post_session.return_value = self.session_create_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})

        session.delete()

        assert_equals(session.ipython_display.send_error.call_count, 1)

    def test_delete_session_when_dead_throws(self):
        self.http_client.post.return_value = self.session_create_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.status = "dead"

        session.delete()

        assert_equals(session.ipython_display.send_error.call_count, 0)

    def test_create_sql_hive_context_happens_once(self):
        kind = constants.SESSION_KIND_SPARK
        ipython_display = MagicMock()

        self.http_client.post_session.return_value = self.session_create_json
        self.post_statement_responses = [self.post_statement_json,
                                         self.post_statement_json]
        self.http_client.post_statement.side_effect = self._next_statement_response_post
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json,
                                        self.ready_statement_json,
                                        self.ready_statement_json]
        self.http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind)
        session.ipython_display = ipython_display
        conf.override_all({})
        session.start(create_sql_context=False)

        # Reset the mock so that post called count is accurate
        self.http_client.reset_mock()

        session.create_sql_context()
        assert ipython_display.writeln.call_count == 2
        assert session.created_sql_context

        # Second call should not issue a post request
        session.create_sql_context()

        assert call(0, {"code": "val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)"}) \
               in self.http_client.post_statement.call_args_list
        assert len(self.http_client.post_statement.call_args_list) == 1

        session._get_sql_context_creation_command = MagicMock()

        session._get_sql_context_creation_command.return_value.execute.return_value = (True, "Success")
        session.created_sql_context = None
        session.create_sql_context()
        assert session.created_sql_context

    def test_create_sql_context_throws_when_command_fails(self):
        kind = constants.SESSION_KIND_SPARK
        ipython_display = MagicMock()

        session = self._create_session(kind=kind)
        session.ipython_display = ipython_display
        session._get_sql_context_creation_command = MagicMock()

        session._get_sql_context_creation_command.return_value.execute.return_value = (False, "Exception")
        session.created_sql_context = None

        try:
            session.create_sql_context()
            assert False
        except FailedToCreateSqlContextException as ex:
            assert_equals(str(ex), "Failed to create the SqlContext.\nError: '{}'".format("Exception"))
            assert session.created_sql_context is None

    def test_create_sql_context_spark(self):
        kind = constants.SESSION_KIND_SPARK
        self.http_client.post_session.return_value = self.session_create_json
        self.post_statement_responses = [self.post_statement_json, self.post_statement_json]
        self.http_client.post_statement.side_effect = self._next_statement_response_post
        self.get_session_responses = [self.ready_sessions_json, self.ready_sessions_json]
        self.http_client.get_session.side_effect = self._next_session_response_get
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json,
                                        self.ready_statement_json]
        self.http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind)
        conf.override_all({})
        session.start()

        assert call(0, {"code": "val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)"}) \
               in self.http_client.post_statement.call_args_list

    def test_create_sql_hive_context_pyspark(self):
        kind = constants.SESSION_KIND_PYSPARK
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.post_statement.return_value = self.post_statement_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json,
                                        self.ready_statement_json]
        self.http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind)
        conf.override_all({})
        session.start()

        assert call(0, {"code": "from pyspark.sql import HiveContext\n"
                                "sqlContext = HiveContext(sc)"}) \
               in self.http_client.post_statement.call_args_list

    @raises(BadUserDataException)
    def test_create_sql_hive_context_unknown_throws(self):
        kind = "unknown"
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.post_statement.return_value = self.post_statement_json
        self.http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json]
        self.http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind)
        conf.override_all({})
        session.start()

    def test_get_sql_context_creation_command_all_langs(self):
        for kind in constants.SESSION_KINDS_SUPPORTED:
            session = self._create_session(kind=kind)
            session._get_sql_context_creation_command()

    def test_start_emits_start_end_session(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()
        session.start()
        conf.override_all({})

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, True, "", "")

    def test_start_emits_start_end_failed_session_when_sql_fails(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock(side_effect=ValueError)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.override_all({})

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, False, "ValueError", "")

    def test_start_emits_start_end_failed_session_when_bad_status(self):
        self.http_client.post_session.side_effect = ValueError
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.override_all({})

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, False, "ValueError", "")

    def test_start_emits_start_end_failed_session_when_wait_for_idle_throws(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind)
        session.create_sql_context = MagicMock()
        session.wait_for_idle = MagicMock(side_effect=ValueError)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.override_all({})

        self.spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        self.spark_events.emit_session_creation_end_event.assert_called_once_with(
            session.guid, kind, session.id, session.status, False, "ValueError", "")

    def test_delete_session_emits_start_end(self):
        self.http_client.post_session.return_value = self.session_create_json
        self.http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)
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

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)
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

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.override_all({})
        session.start(create_sql_context=False)
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
        self._verify_get_app_id("null", None)

    def test_get_missing_app_id(self):
        self._verify_get_app_id(None, None)

    def test_get_normal_app_id(self):
        self._verify_get_app_id("\"app_id_123\"", "app_id_123")

    def _verify_get_app_id(self, mock_app_id, expected_app_id):
        mock_field = ",\"appId\":" + mock_app_id if mock_app_id is not None else ""
        get_session_json = json.loads('{"id":0,"state":"idle","output":null%s}' % mock_field)
        session = self._create_session_with_fixed_get_response(get_session_json)

        app_id = session.get_app_id()

        assert_equals(expected_app_id, app_id)
        assert_equals(2, self.http_client.get_session.call_count)

    def test_get_empty_driver_log_url(self):
        self._verify_get_driver_log_url("null", None)

    def test_get_normal_driver_log_url(self):
        self._verify_get_driver_log_url("\"http://example.com\"", "http://example.com")

    def test_missing_app_info_get_driver_log_url(self):
        self._verify_get_driver_log_url_json(self.ready_sessions_json, None)

    def _verify_get_driver_log_url(self, mock_driver_log_url, expected_url):
        mock_field = "\"driverLogUrl\":" + mock_driver_log_url if mock_driver_log_url is not None else ""
        session_json = json.loads('{"id":0,"state":"idle","output":null,"appInfo":{%s}}' % mock_field)
        self._verify_get_driver_log_url_json(session_json, expected_url)

    def _verify_get_driver_log_url_json(self, get_session_json, expected_url):
        session = self._create_session_with_fixed_get_response(get_session_json)

        driver_log_url = session.get_driver_log_url()

        assert_equals(expected_url, driver_log_url)
        assert_equals(2, self.http_client.get_session.call_count)

    def test_get_empty_spark_ui_url(self):
        self._verify_get_spark_ui_url("null", None)

    def test_get_normal_spark_ui_url(self):
        self._verify_get_spark_ui_url("\"http://example.com\"", "http://example.com")

    def test_missing_app_info_get_spark_ui_url(self):
        self._verify_get_spark_ui_url_json(self.ready_sessions_json, None)

    def _verify_get_spark_ui_url(self, mock_spark_ui_url, expected_url):
        mock_field = "\"sparkUiUrl\":" + mock_spark_ui_url if mock_spark_ui_url is not None else ""
        session_json = json.loads('{"id":0,"state":"idle","output":null,"appInfo":{%s}}' % mock_field)
        self._verify_get_spark_ui_url_json(session_json, expected_url)

    def _verify_get_spark_ui_url_json(self, get_session_json, expected_url):
        session = self._create_session_with_fixed_get_response(get_session_json)

        spark_ui_url = session.get_spark_ui_url()

        assert_equals(expected_url, spark_ui_url)
        assert_equals(2, self.http_client.get_session.call_count)
