import json

from mock import MagicMock, call
from nose.tools import raises, assert_equals

from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.livyclientlib.livyclienttimeouterror import LivyClientTimeoutError
from remotespark.livyclientlib.livyunexpectedstatuserror import LivyUnexpectedStatusError
from remotespark.livyclientlib.livysession import LivySession
import remotespark.utils.configuration as conf
from remotespark.utils.utils import get_connection_string
from remotespark.utils.constants import Constants


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text
    
    def json(self):
        return json.loads(self._json_text)

    @property
    def status_code(self):
        return self._status_code


class TestLivySession:

    def __init__(self):
        self.pi_result = "Pi is roughly 3.14336"

        self.session_create_json = '{"id":0,"state":"starting","kind":"spark","log":[]}'
        self.ready_sessions_json = '{"from":0,"total":1,"sessions":[{"id":0,"state":"idle","kind":"spark","log":[""]}]}'
        self.error_sessions_json = '{"from":0,"total":1,"sessions":[{"id":0,"state":"error","kind":"spark","log":' \
                                   '[""]}]}'
        self.busy_sessions_json = '{"from":0,"total":1,"sessions":[{"id":0,"state":"busy","kind":"spark","log":[""]}]}'
        self.post_statement_json = '{"id":0,"state":"running","output":null}'
        self.running_statement_json = '{"total_statements":1,"statements":[{"id":0,"state":"running","output":null}]}'
        self.ready_statement_json = '{"total_statements":1,"statements":[{"id":0,"state":"available","output":{"statu' \
                                    's":"ok","execution_count":0,"data":{"text/plain":"Pi is roughly 3.14336"}}}]}'

        self.get_responses = []
        self.post_responses = []

    def _next_response_get(self, *args):
        val = self.get_responses[0]
        self.get_responses = self.get_responses[1:]
        return val

    def _next_response_post(self, *args):
        val = self.post_responses[0]
        self.post_responses = self.post_responses[1:]    
        return val

    def _create_session(self, kind=Constants.session_kind_spark, session_id="-1", sql_created=False, http_client=None):
        if http_client is None:
            http_client = MagicMock()

        ipython_display = MagicMock()

        return LivySession(ipython_display, http_client, session_id, sql_created, {"kind": kind})

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
        conf.load({})

    @raises(AssertionError)
    def test_constructor_throws_sql_create_timeout_seconds(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 0
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

    def test_constructor_starts_with_existing_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session_id = "1"
        session = self._create_session(session_id=session_id, sql_created=True)
        conf.load()

        assert session.id == session_id
        assert session.started_sql_context

    def test_constructor_starts_with_no_session(self):
        conf.override_all({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session = self._create_session()
        conf.load()

        assert session.id == "-1"
        assert not session.started_sql_context

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
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = Constants.session_kind_spark
        session = self._create_session(kind=kind, http_client=http_client)
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("starting", session._status)
        assert_equals("0", session.id)
        http_client.post.assert_called_with(
            "/sessions", [201], {"kind": "spark"})

    def test_start_r_starts_session(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = Constants.session_kind_sparkr
        session = self._create_session(kind=kind, http_client=http_client)
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("starting", session._status)
        assert_equals("0", session.id)
        http_client.post.assert_called_with(
            "/sessions", [201], {"kind": "sparkr"})

    def test_start_python_starts_session(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = Constants.session_kind_pyspark
        session = self._create_session(kind=kind, http_client=http_client)
        session.start()
        conf.load()

        assert_equals(kind, session.kind)
        assert_equals("starting", session._status)
        assert_equals("0", session.id)
        http_client.post.assert_called_with(
            "/sessions", [201], {"kind": "pyspark"})

    def test_start_passes_in_all_properties(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        kind = Constants.session_kind_spark
        properties = {"kind": kind, "extra": 1}

        ipython_display = MagicMock()
        session = LivySession(ipython_display, http_client, "-1", False, properties)
        session.start()
        conf.load()

        http_client.post.assert_called_with(
            "/sessions", [201], properties)

    def test_status_gets_latest(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        http_client.get.return_value = DummyResponse(200, self.ready_sessions_json)
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.start()

        session.refresh_status()
        state = session._status

        assert_equals("idle", state)
        http_client.get.assert_called_with("/sessions", [200])

    def test_wait_for_idle_returns_when_in_state(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.ready_sessions_json)]
        http_client.get.side_effect = self._next_response_get

        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.override_all({})

        session.start()

        session.wait_for_idle(30)

        http_client.get.assert_called_with("/sessions", [200])
        assert_equals(2, http_client.get.call_count)

    @raises(LivyUnexpectedStatusError)
    def test_wait_for_idle_throws_when_in_final_status(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.error_sessions_json)]
        http_client.get.side_effect = self._next_response_get

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.start()

        session.wait_for_idle(30)

    @raises(LivyClientTimeoutError)
    def test_wait_for_idle_times_out(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.ready_sessions_json)]
        http_client.get.side_effect = self._next_response_get

        conf.override_all({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.start()

        session.wait_for_idle(0.01)

    def test_delete_session_when_active(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session.start()

        session.delete()

        assert_equals("dead", session._status)

    @raises(ValueError)
    def test_delete_session_when_not_started(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()

        session.delete()
    
        assert_equals("dead", session._status)
        assert_equals("-1", session.id)

    @raises(ValueError)
    def test_delete_session_when_dead_throws(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(http_client=http_client)
        conf.load()
        session._status = "dead"

        session.delete()

    def test_execute(self):
        kind = Constants.session_kind_spark
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()
        command = "command"

        result = session.execute(command)

        http_client.post.assert_called_with("/sessions/0/statements", [201], {"code": command})
        http_client.get.assert_called_with("/sessions/0/statements", [200])
        assert_equals(2, http_client.get.call_count)
        assert result[0]
        assert_equals(self.pi_result, result[1])

    def test_create_sql_hive_context_happens_once(self):
        kind = Constants.session_kind_spark
        http_client = MagicMock()
        ipython_display = MagicMock()

        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json),
                              DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        session.ipython_display = ipython_display
        conf.load()
        session.start()

        # Reset the mock so that post called count is accurate
        http_client.reset_mock()

        session.create_sql_context()
        assert ipython_display.writeln.call_count == 3

        # Second call should not issue a post request
        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "val sqlContext = new org.apache.spark.sql.SQLContext"
                                                              "(sc)\nimport sqlContext.implicits._"}) \
               in http_client.post.call_args_list
        assert call("/sessions/0/statements", [201], {"code": "val hiveContext = new org.apache.spark.sql.hive.Hive"
                                                              "Context(sc)"}) in http_client.post.call_args_list
        assert len(http_client.post.call_args_list) == 2

    def test_create_sql_context_spark(self):
        kind = Constants.session_kind_spark
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json),
                              DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "val sqlContext = new org.apache.spark.sql.SQLContext"
                                                              "(sc)\nimport sqlContext.implicits._"}) \
               in http_client.post.call_args_list
        assert call("/sessions/0/statements", [201], {"code": "val hiveContext = new org.apache.spark.sql.hive.Hive"
                                                              "Context(sc)"}) in http_client.post.call_args_list

    def test_create_sql_hive_context_pyspark(self):
        kind = Constants.session_kind_pyspark
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json),
                              DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "from pyspark.sql import SQLContext\nfrom pyspark."
                                                              "sql.types import *\nsqlContext = SQLContext("
                                                              "sc)"}) in http_client.post.call_args_list
        assert call("/sessions/0/statements", [201], {"code": "from pyspark.sql import HiveContext\n"
                                                              "hiveContext = HiveContext(sc)"}) \
               in http_client.post.call_args_list

    @raises(ValueError)
    def test_create_sql_hive_context_unknown_throws(self):
        kind = "unknown"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()
        session.start()

        session.create_sql_context()

    def test_serialize(self):
        url = "url"
        username = "username"
        password = "password"
        connection_string = get_connection_string(url, username, password)
        http_client = MagicMock()
        http_client.connection_string = connection_string
        kind = Constants.session_kind_spark
        conf.override_all({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = self._create_session(kind=kind, http_client=http_client)
        conf.load()

        serialized = session.get_state().to_dict()

        assert serialized["connectionstring"] == connection_string
        assert serialized["id"] == "-1"
        assert serialized["kind"] == kind
        assert serialized["sqlcontext"] == False
        assert serialized["version"] == "0.0.0"
        assert len(serialized.keys()) == 5

    def test_get_sql_context_creation_command_all_langs(self):
        for kind in Constants.session_kinds_supported:
            session = self._create_session(kind=kind)
            session._get_sql_context_creation_command()

    def test_get_hive_context_creation_command_all_langs(self):
        for kind in Constants.session_kinds_supported:
            session = self._create_session(kind=kind)
            session._get_hive_context_creation_command()
