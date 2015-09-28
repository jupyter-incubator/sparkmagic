import json

from nose.tools import raises, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.livysession import LivySession


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

    def test_start_starts_session(self):
        kind = "spark"
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)

        session = LivySession(http_client, kind)
        session.start()
    
        assert_equals("spark", session.kind)
        assert_equals("starting", session._state)
        assert_equals("0", session._id)
        http_client.post.assert_called_with("/sessions", [201], {"kind": kind})

    def test_state_gets_latest(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        http_client.get.return_value = DummyResponse(200, self.ready_sessions_json)
        session = LivySession(http_client, "spark")
        session.start()
    
        state = session.state

        assert_equals("idle", state)
        http_client.get.assert_called_with("/sessions", [200])

    def test_wait_for_state_returns_when_in_state(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        self.get_responses = [DummyResponse(200, self.busy_sessions_json),
                              DummyResponse(200, self.ready_sessions_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, "spark")
        session.start()

        session.wait_for_state("idle")

        http_client.get.assert_called_with("/sessions", [200])
        assert_equals(2, http_client.get.call_count)

    def test_delete_session_when_active(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        session = LivySession(http_client, "spark")
        session.start()

        session.delete()

        assert_equals("dead", session._state)

    @raises(ValueError)
    def test_delete_session_when_not_started(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        session = LivySession(http_client, "spark")

        session.delete()
    
        assert_equals("dead", session._state)

    @raises(ValueError)
    def test_delete_session_when_dead_throws(self):
        http_client = MagicMock()
        http_client.post.return_value = DummyResponse(201, self.session_create_json)
        session = LivySession(http_client, "spark")
        session._state = "dead"

        session.delete()

    def test_execute(self):
        kind = "spark"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, kind)
        session.start()
        command = "command"

        result = session.execute(command)

        http_client.post.assert_called_with("/sessions/0/statements", [201], {"code": command})
        http_client.get.assert_called_with("/sessions/0/statements", [200])
        assert_equals(2, http_client.get.call_count)
        assert_equals(self.pi_result, result)

    def test_create_sql_context_happens_once(self):
        kind = "spark"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, kind)
        session.start()

        # Reset the mock so that post called count is accurate
        http_client.reset_mock()

        session.create_sql_context()

        # Second call should not issue a post request
        session.create_sql_context()

        http_client.post.assert_called_once_with("/sessions/0/statements", [201],
                                                 {"code": "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n"
                                                          "import sqlContext.implicits._"})

    def test_create_sql_context_spark(self):
        kind = "spark"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, kind)
        session.start()

        session.create_sql_context()

        http_client.post.assert_called_with("/sessions/0/statements", [201],
                                            {"code": "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n"
                                                     "import sqlContext.implicits._"})

    def test_create_sql_context_pyspark(self):
        kind = "pyspark"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, kind)
        session.start()

        session.create_sql_context()

        http_client.post.assert_called_with("/sessions/0/statements", [201],
                                            {"code": "from pyspark.sql import SQLContext\n"
                                                     "from pyspark.sql.types import *\n"
                                                     "sqlContext = SQLContext(sc)"})

    @raises(ValueError)
    def test_create_sql_context_unknown_throws(self):
        kind = "unknown"
        http_client = MagicMock()
        self.post_responses = [DummyResponse(201, self.session_create_json),
                               DummyResponse(201, self.post_statement_json)]
        http_client.post.side_effect = self._next_response_post
        self.get_responses = [DummyResponse(200, self.ready_sessions_json),
                              DummyResponse(200, self.running_statement_json),
                              DummyResponse(200, self.ready_statement_json)]
        http_client.get.side_effect = self._next_response_get
        session = LivySession(http_client, kind)
        session.start()

        session.create_sql_context()
