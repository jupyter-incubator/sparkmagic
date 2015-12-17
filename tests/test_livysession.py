import json
from nose.tools import raises, assert_equals
from mock import MagicMock, call

from remotespark.livyclientlib.livysession import LivySession
from remotespark.livyclientlib.livyclienttimeouterror import LivyClientTimeoutError
from remotespark.livyclientlib.utils import get_connection_string, get_instance_id
from remotespark.livyclientlib.configuration import _t_config_hook


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text
    
    def json(self):
        return json.loads(self._json_text)

    def status_code(self):
        return self._status_code


class TestLivySession:
    pi_result = "Pi is roughly 3.14336"

    session_create_json = '{"id":0,"state":"starting","kind":"spark","log":[]}'
    ready_sessions_json = '{"from":0,"total":1,"sessions":[{"id":0,"state":"idle","kind":"spark","log":["16:23:01,15' \
                          '1 |-INFO in ch.qos.logback.core.joran.action.AppenderAction - Naming appender as [STDOUT]' \
                          '","16:23:01,213 |-INFO in ch.qos.logback.core.joran.action.NestedComplexPropertyIA - As' \
                          'suming default type [ch.qos.logback.access.PatternLayoutEncoder] for [encoder] propert' \
                          'y","16:23:01,368 |-INFO in ch.qos.logback.core.joran.action.AppenderRefAction - Attachin' \
                          'g appender named [STDOUT] to null","16:23:01,368 |-INFO in ch.qos.logback.access.joran.act' \
                          'ion.ConfigurationAction - End of configuration.","16:23:01,371 |-INFO in ch.qos.logback.ac' \
                          'cess.joran.JoranConfigurator@53799e55 - Registering current configuration as safe fallback' \
                          ' point","","15/09/04 16:23:01 INFO server.ServerConnector: Started ServerConnector@388859' \
                          'e4{HTTP/1.1}{0.0.0.0:37394}","15/09/04 16:23:01 INFO server.Server: Started @27514ms","' \
                          '15/09/04 16:23:01 INFO livy.WebServer: Starting server on 37394","Starting livy-repl on' \
                          ' http://10.0.0.11:37394"]}]}'
    busy_sessions_json = '{"from":0,"total":1,"sessions":[{"id":0,"state":"busy","kind":"spark","log":["16:23:01,151' \
                         ' |-INFO in ch.qos.logback.core.joran.action.AppenderAction - Naming appender as [STDOUT]",' \
                         '"16:23:01,213 |-INFO in ch.qos.logback.core.joran.action.NestedComplexPropertyIA - Assumin' \
                         'g default type [ch.qos.logback.access.PatternLayoutEncoder] for [encoder] property","16:23' \
                         ':01,368 |-INFO in ch.qos.logback.core.joran.action.AppenderRefAction - Attaching appender ' \
                         'named [STDOUT] to null","16:23:01,368 |-INFO in ch.qos.logback.access.joran.action.Configu' \
                         'rationAction - End of configuration.","16:23:01,371 |-INFO in ch.qos.logback.access.joran.' \
                         'JoranConfigurator@53799e55 - Registering current configuration as safe fallback point","",' \
                         '"15/09/04 16:23:01 INFO server.ServerConnector: Started ServerConnector@388859e4{HTTP/1.1}' \
                         '{0.0.0.0:37394}","15/09/04 16:23:01 INFO server.Server: Started @27514ms","15/09/04 16:23:' \
                         '01 INFO livy.WebServer: Starting server on 37394","Starting livy-repl on http://10.0.0.11:' \
                         '37394"]}]}'
    post_statement_json = '{"id":0,"state":"running","output":null}'
    running_statement_json = '{"total_statements":1,"statements":[{"id":0,"state":"running","output":null}]}'
    ready_statement_json = '{"total_statements":1,"statements":[{"id":0,"state":"available","output":{"status":"ok",' \
                           '"execution_count":0,"data":{"text/plain":"Pi is roughly 3.14336"}}}]}'
    
    get_responses = []
    post_responses = []

    def _next_response_get(self, *args):
        val = self.get_responses[0]
        self.get_responses = self.get_responses[1:]
        return val

    def _next_response_post(self, *args):
        val = self.post_responses[0]
        self.post_responses = self.post_responses[1:]    
        return val

    @raises(AssertionError)
    def test_constructor_throws_status_sleep_seconds(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "-1"
        sql_created = False
        _t_config_hook({
            "status_sleep_seconds": 0,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

    @raises(AssertionError)
    def test_constructor_throws_statement_sleep_seconds(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "-1"
        sql_created = False
        _t_config_hook({
            "status_sleep_seconds": 3,
            "statement_sleep_seconds": 0,
            "create_sql_context_timeout_seconds": 60
        })
        LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

    @raises(AssertionError)
    def test_constructor_throws_sql_create_timeout_seconds(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "-1"
        sql_created = False
        _t_config_hook({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 0
        })
        LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

    @raises(ValueError)
    def test_constructor_throws_invalid_session_sql_combo(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "-1"
        sql_created = True
        _t_config_hook({
            "status_sleep_seconds": 2,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

    def test_constructor_starts_with_existing_session(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "1"
        sql_created = True
        _t_config_hook({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session = LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

        assert session.id == "1"
        assert session.started_sql_context

    def test_constructor_starts_with_no_session(self):
        kind = "scala"
        http_client = MagicMock()
        session_id = "-1"
        sql_created = False
        _t_config_hook({
            "status_sleep_seconds": 4,
            "statement_sleep_seconds": 2,
            "create_sql_context_timeout_seconds": 60
        })
        session = LivySession(http_client, kind, session_id, sql_created)
        _t_config_hook({})

        assert session.id == "-1"
        assert not session.started_sql_context

    def test_is_final_status(self):
        kind = "scala"
        http_client = MagicMock()

        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})

        assert not session.is_final_status("idle")
        assert not session.is_final_status("starting")
        assert not session.is_final_status("busy")

        assert session.is_final_status("dead")
        assert session.is_final_status("error")

    def test_start_scala_starts_session(self):
        kind = "scala"
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        session.start()
        _t_config_hook({})

        assert_equals(kind, session.language)
        assert_equals("starting", session._status)
        assert_equals("0", session.id)
        http_client.post.assert_called_with(
            "/sessions", [201], {"kind": "spark", "name": "remotesparkmagics_{}".format(get_instance_id())})

    def test_start_python_starts_session(self):
        kind = "python"
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        session.start()
        _t_config_hook({})

        assert_equals(kind, session.language)
        assert_equals("starting", session._status)
        assert_equals("0", session.id)
        http_client.post.assert_called_with(
            "/sessions", [201],{"kind": "pyspark", "name": "remotesparkmagics_{}".format(get_instance_id())})

    def test_status_gets_latest(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        http_client.get.return_value = DummyResponse(200, self.ready_sessions_json)
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})
        session.start()
    
        state = session.status

        assert_equals("idle", state)
        http_client.get.assert_called_with("/sessions", [200])

    def test_wait_for_status_returns_when_in_state(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.ready_sessions_json)]
        http_client.get.side_effect = self._next_response_get

        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})

        session.start()

        session.wait_for_status("idle", 30)

        http_client.get.assert_called_with("/sessions", [200])
        assert_equals(2, http_client.get.call_count)

    @raises(LivyClientTimeoutError)
    def test_wait_for_status_times_out(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.ready_sessions_json)]
        http_client.get.side_effect = self._next_response_get

        _t_config_hook({
            "status_sleep_seconds": 0.011,
            "statement_sleep_seconds": 6000
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})

        session.start()

        session.wait_for_status("idle", 0.01)

    def test_delete_session_when_active(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})
        session.start()

        session.delete()

        assert_equals("dead", session._status)

    @raises(ValueError)
    def test_delete_session_when_not_started(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})

        session.delete()
    
        assert_equals("dead", session._status)
        assert_equals("-1", session.id)

    @raises(ValueError)
    def test_delete_session_when_dead_throws(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, "scala", "-1", False)
        _t_config_hook({})
        session._status = "dead"

        session.delete()

    def test_execute(self):
        kind = "scala"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})
        session.start()
        command = "command"

        result = session.execute(command)

        http_client.post.assert_called_with("/sessions/0/statements", [201], {"code": command})
        http_client.get.assert_called_with("/sessions/0/statements", [200])
        assert_equals(2, http_client.get.call_count)
        assert_equals(self.pi_result, result)

    def test_create_sql_hive_context_happens_once(self):
        kind = "scala"
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
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})
        session.start()

        # Reset the mock so that post called count is accurate
        http_client.reset_mock()

        session.create_sql_context()

        # Second call should not issue a post request
        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "val sqlContext = new org.apache.spark.sql.SQLContext"
                                                              "(sc)\nimport sqlContext.implicits._"}) \
               in http_client.post.call_args_list
        assert call("/sessions/0/statements", [201], {"code": "val hiveContext = new org.apache.spark.sql.hive.Hive"
                                                              "Context(sc)"}) \
               in http_client.post.call_args_list
        assert len(http_client.post.call_args_list) == 2

    def test_create_sql_context_spark(self):
        kind = "scala"
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
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})
        session.start()

        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "val sqlContext = new org.apache.spark.sql.SQLContext"
                                                              "(sc)\nimport sqlContext.implicits._"}) \
               in http_client.post.call_args_list
        assert call("/sessions/0/statements", [201], {"code": "val hiveContext = new org.apache.spark.sql.hive.Hive"
                                                              "Context(sc)"}) \
               in http_client.post.call_args_list


    def test_create_sql_hive_context_pyspark(self):
        kind = "python"
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
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})
        session.start()

        session.create_sql_context()

        assert call("/sessions/0/statements", [201], {"code": "from pyspark.sql import SQLContext\n"
                                                              "from pyspark.sql.types import *\n"
                                                              "sqlContext = SQLContext(sc)"}) \
               in http_client.post.call_args_list
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
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})
        session.start()

        session.create_sql_context()

    def test_serialize(self):
        url = "url"
        username = "username"
        password = "password"
        connection_string = get_connection_string(url, username, password)
        http_client = MagicMock()
        http_client.connection_string = connection_string
        kind = "scala"
        _t_config_hook({
            "status_sleep_seconds": 0.01,
            "statement_sleep_seconds": 0.01
        })
        session = LivySession(http_client, kind, "-1", False)
        _t_config_hook({})

        serialized = session.get_state().to_dict()

        assert serialized["connectionstring"] == connection_string
        assert serialized["id"] == "-1"
        assert serialized["language"] == kind
        assert serialized["sqlcontext"] == False
        assert serialized["version"] == "0.0.0"
        assert len(serialized.keys()) == 5
