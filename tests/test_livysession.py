import json
from mock import MagicMock, call
from nose.tools import raises, assert_equals

from remotespark.livyclientlib.livyclienttimeouterror import LivyClientTimeoutError
from remotespark.livyclientlib.livyunexpectedstatuserror import LivyUnexpectedStatusError
from remotespark.livyclientlib.livysession import LivySession
import remotespark.utils.configuration as conf
import remotespark.utils.constants as constants


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text

    def json(self):
        return json.loads(self._json_text)

    @property
    def status_code(self):
        return self._status_code


CONN_STR = 'url=https://www.DFAS90D82309F0W9ASD0F9ZX.com;username=abcd;password=1234'


class TestLivySession(object):
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
        self.get_statement_responses = []
        self.post_statement_responses = []
        self.get_session_responses = []
        self.post_session_responses = []

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

    @staticmethod
    def _create_session(kind=constants.SESSION_KIND_SPARK, session_id=-1,
                        sql_created=False, http_client=None, spark_events=None):
        if http_client is None:
            http_client = MagicMock()
        if spark_events is None:
            spark_events = MagicMock()
        ipython_display = MagicMock()
        session = LivySession(http_client, {"kind": kind}, ipython_display, session_id, sql_created, spark_events)
        return session

    @raises(AssertionError)
    def test_constructor_throws_status_sleep_seconds(self):
        conf.override_all({
            "status_sleep_seconds": 0,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session()
        conf.load()

    @raises(AssertionError)
    def test_constructor_throws_statement_sleep_seconds(self):
        conf.override_all({
            "status_sleep_seconds": 3,
            "statement_sleep_seconds": 0,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session()
        conf.load()

    @raises(ValueError)
    def test_constructor_throws_invalid_session_sql_combo(self):
        conf.override_all({
            "status_sleep_seconds": 2,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        self._create_session(sql_created=True)
        conf.load()

    def test_doesnt_do_anything_or_create_sql_context_automatically(self):
        # If the session object does anything (attempts to create a session or run
        # a statement), the http_client will fail
        http_client = MagicMock(side_effect=ValueError)
        self._create_session(http_client=http_client)

    def test_constructor_starts_with_existing_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session_id = 1
        session = self._create_session(session_id=session_id, sql_created=True)
        conf.load()

        assert session.id == session_id
        assert session.created_sql_context

    def test_constructor_starts_with_no_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session = self._create_session()
        conf.load()

        assert session.id == -1
        assert not session.created_sql_context

    def test_is_final_status(self):
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session()
        conf.load()

        assert not session.is_final_status("idle")
        assert not session.is_final_status("starting")
        assert not session.is_final_status("busy")

        assert session.is_final_status("dead")
        assert session.is_final_status("error")

    def test_start_scala_starts_session(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind, http_client=http_client)
        session.create_sql_context = MagicMock()
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        http_client.post_session.assert_called_with({"kind": "spark"})
        session.create_sql_context.assert_called_once_with()

    def test_start_r_starts_session(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARKR
        session = self._create_session(kind=kind, http_client=http_client)
        session.create_sql_context = MagicMock()
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        http_client.post_session.assert_called_with({"kind": "sparkr"})
        session.create_sql_context.assert_called_once_with()

    def test_start_python_starts_session(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_PYSPARK
        session = self._create_session(kind=kind, http_client=http_client)
        session.create_sql_context = MagicMock()
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        http_client.post_session.assert_called_with({"kind": "pyspark"})
        session.create_sql_context.assert_called_once_with()

    def test_start_turn_off_sql_context_creation(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_PYSPARK
        session = self._create_session(kind=kind, http_client=http_client)
        session.create_sql_context = MagicMock(side_effect=ValueError)
        session.start(create_sql_context=False)
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("idle", session.status)
        assert_equals(0, session.id)
        http_client.post_session.assert_called_with({"kind": "pyspark"})
        session.create_sql_context.assert_not_called()

    def test_start_passes_in_all_properties(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        properties = {"kind": kind, "extra": 1}

        ipython_display = MagicMock()
        session = LivySession(http_client, properties, ipython_display)
        session.start(create_sql_context=False)
        conf.load()

        http_client.post_session.assert_called_with(properties)

    def test_status_gets_latest_status(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.start(create_sql_context=False)

        session._refresh_status()
        state = session.status

        assert_equals("idle", state)
        http_client.get_session.assert_called_with(0)

    def test_logs_gets_latest_logs(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json
        http_client.get_all_session_logs.return_value = self.log_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.start(create_sql_context=False)

        logs = session.get_logs()

        assert_equals("hi\nhi", logs)
        http_client.get_all_session_logs.assert_called_with(0)

    def test_wait_for_idle_returns_when_in_state(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        http_client.get_session.side_effect = self._next_session_response_get

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.override_all({})

        session.start(create_sql_context=False)

        session.wait_for_idle(30)

        http_client.get_session.assert_called_with(0)
        assert_equals(3, http_client.get_session.call_count)

    @raises(LivyUnexpectedStatusError)
    def test_wait_for_idle_throws_when_in_final_status(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.error_sessions_json]
        http_client.get_session.side_effect = self._next_session_response_get
        http_client.get_all_session_logs.return_value = self.log_json

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.start(create_sql_context=False)

        session.wait_for_idle(30)

    @raises(LivyClientTimeoutError)
    def test_wait_for_idle_times_out(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        self.get_session_responses = [self.ready_sessions_json,
                                      self.busy_sessions_json,
                                      self.busy_sessions_json,
                                      self.ready_sessions_json]
        http_client.get_session.side_effect = self._next_session_response_get

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.start(create_sql_context=False)

        session.wait_for_idle(0.01)

    def test_delete_session_when_active(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.start(create_sql_context=False)

        session.delete()

        assert_equals("dead", session.status)

    @raises(ValueError)
    def test_delete_session_when_not_started(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.delete()

        assert_equals("dead", session.status)
        assert_equals(-1, session.id)

    @raises(ValueError)
    def test_delete_session_when_dead_throws(self):
        http_client = MagicMock()
        http_client.post.return_value = self.session_create_json
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.status = "dead"

        session.delete()

    def test_create_sql_hive_context_happens_once(self):
        kind = constants.SESSION_KIND_SPARK
        http_client = MagicMock()
        ipython_display = MagicMock()

        http_client.post_session.return_value = self.session_create_json
        self.post_statement_responses = [self.post_statement_json,
                                         self.post_statement_json]
        http_client.post_statement.side_effect = self._next_statement_response_post
        http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json,
                                        self.ready_statement_json,
                                        self.ready_statement_json]
        http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        session.ipython_display = ipython_display
        conf.load()
        session.start(create_sql_context=False)

        # Reset the mock so that post called count is accurate
        http_client.reset_mock()

        session.create_sql_context()
        assert ipython_display.writeln.call_count == 2
        assert session.created_sql_context

        # Second call should not issue a post request
        session.create_sql_context()

        assert call(0, {"code": "val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)"}) \
               in http_client.post_statement.call_args_list
        assert len(http_client.post_statement.call_args_list) == 1

        session._get_sql_context_creation_command = MagicMock()

        session._get_sql_context_creation_command.return_value.execute.return_value = (True, "Success")
        session.created_sql_context = None
        session.create_sql_context()
        assert session.created_sql_context

    def test_create_sql_context_throws_when_command_fails(self):
        kind = constants.SESSION_KIND_SPARK
        http_client = MagicMock()
        ipython_display = MagicMock()

        session = self._create_session(kind=kind, http_client=http_client)
        session.ipython_display = ipython_display
        session._get_sql_context_creation_command = MagicMock()

        session._get_sql_context_creation_command.return_value.execute.return_value = (False, "Exception")
        session.created_sql_context = None

        try:
            session.create_sql_context()
            assert False
        except ValueError as ex:
            assert str(ex) == "Failed to create the SqlContext.\nError, '{}'".format("Exception")
            assert session.created_sql_context is None

    def test_create_sql_context_spark(self):
        kind = constants.SESSION_KIND_SPARK
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        self.post_statement_responses = [self.post_statement_json, self.post_statement_json]
        http_client.post_statement.side_effect = self._next_statement_response_post
        self.get_session_responses = [self.ready_sessions_json, self.ready_sessions_json]
        http_client.get_session.side_effect = self._next_session_response_get
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json,
                                        self.ready_statement_json]
        http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

        assert call(0, {"code": "val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)"}) \
               in http_client.post_statement.call_args_list

    def test_create_sql_hive_context_pyspark(self):
        kind = constants.SESSION_KIND_PYSPARK
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.post_statement.return_value = self.post_statement_json
        http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json,
                                        self.ready_statement_json]
        http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

        assert call(0, {"code": "from pyspark.sql import HiveContext\n"
                                "sqlContext = HiveContext(sc)"}) \
               in http_client.post_statement.call_args_list

    @raises(ValueError)
    def test_create_sql_hive_context_unknown_throws(self):
        kind = "unknown"
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.post_statement.return_value = self.post_statement_json
        http_client.get_session.return_value = self.ready_sessions_json
        self.get_statement_responses = [self.running_statement_json, self.ready_statement_json]
        http_client.get_statement.side_effect = self._next_statement_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

    def test_get_sql_context_creation_command_all_langs(self):
        for kind in constants.SESSION_KINDS_SUPPORTED:
            session = self._create_session(kind=kind)
            session._get_sql_context_creation_command()

    def test_start_emits_start_end_session(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind, http_client=http_client, spark_events=spark_events)
        session.create_sql_context = MagicMock()
        session.start()
        conf.load()

        spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        spark_events.emit_session_creation_end_event.assert_called_once_with(session.guid, kind, session.id,
                                                                             session.status, True, "", "")

    def test_start_emits_start_end_failed_session_when_sql_fails(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind, http_client=http_client, spark_events=spark_events)
        session.create_sql_context = MagicMock(side_effect=ValueError)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.load()

        spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        spark_events.emit_session_creation_end_event.assert_called_once_with(session.guid, kind, session.id,
                                                                             session.status, False, "ValueError", "")

    def test_start_emits_start_end_failed_session_when_bad_status(self):
        http_client = MagicMock()
        http_client.post_session.side_effect = ValueError
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind, http_client=http_client, spark_events=spark_events)
        session.create_sql_context = MagicMock()

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.load()

        spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        spark_events.emit_session_creation_end_event.assert_called_once_with(session.guid, kind, session.id,
                                                                             session.status, False, "ValueError", "")

    def test_start_emits_start_end_failed_session_when_wait_for_idle_throws(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = constants.SESSION_KIND_SPARK
        session = self._create_session(kind=kind, http_client=http_client, spark_events=spark_events)
        session.create_sql_context = MagicMock()
        session.wait_for_idle = MagicMock(side_effect=ValueError)

        try:
            session.start()
            assert False
        except ValueError:
            pass

        conf.load()

        spark_events.emit_session_creation_start_event.assert_called_once_with(session.guid, kind)
        spark_events.emit_session_creation_end_event.assert_called_once_with(session.guid, kind, session.id,
                                                                             session.status, False, "ValueError", "")

    def test_delete_session_emits_start_end(self):
        http_client = MagicMock()
        http_client.post_session.return_value = self.session_create_json
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client, spark_events=spark_events)
        conf.load()
        session.start(create_sql_context=False)
        end_id = session.id
        end_status = constants.BUSY_SESSION_STATUS
        session.status = end_status

        session.delete()

        assert_equals(session.id, -1)
        spark_events.emit_session_deletion_start_event.assert_called_once_with(session.guid, session.kind, end_id,
                                                                               end_status)
        spark_events.emit_session_deletion_end_event.assert_called_once_with(session.guid, session.kind, end_id,
                                                                             constants.DEAD_SESSION_STATUS,
                                                                             True, "", "")

    def test_delete_session_emits_start_failed_end_when_delete_throws(self):
        http_client = MagicMock()
        http_client.delete_session.side_effect = ValueError
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client, spark_events=spark_events)
        conf.load()
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

        spark_events.emit_session_deletion_start_event.assert_called_once_with(session.guid, session.kind, end_id,
                                                                               end_status)
        spark_events.emit_session_deletion_end_event.assert_called_once_with(session.guid, session.kind, end_id,
                                                                             end_status,
                                                                             False, "ValueError", "")

    def test_delete_session_emits_start_failed_end_when_in_bad_state(self):
        http_client = MagicMock()
        http_client.get_session.return_value = self.ready_sessions_json

        spark_events = MagicMock()

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client, spark_events=spark_events)
        conf.load()
        session.start(create_sql_context=False)
        session.id = 0
        end_id = session.id
        end_status = constants.DEAD_SESSION_STATUS
        session.status = end_status

        try:
            session.delete()
            assert False
        except ValueError:
            pass

        spark_events.emit_session_deletion_start_event.assert_called_once_with(session.guid, session.kind, end_id,
                                                                               end_status)
        spark_events.emit_session_deletion_end_event.assert_called_once_with(
            session.guid, session.kind, session.id, constants.DEAD_SESSION_STATUS, False, "ValueError",
            "Cannot delete session {} that is in state '{}'.".format(session.id, end_status))
