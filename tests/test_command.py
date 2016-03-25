from mock import MagicMock
from nose.tools import assert_equals

from remotespark.livyclientlib.command import Command
from remotespark.utils.constants import SESSION_KIND_SPARK
import remotespark.utils.configuration as conf
from . import test_livysession as tls


def test_execute():
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_json
    conf.override_all({
        "status_sleep_seconds": 0.01,
        "statement_sleep_seconds": 0.01
    })
    session = tls.TestLivySession._create_session(kind=kind, http_client=http_client)
    conf.load()
    session.start(create_sql_context=False)
    command = Command("command")
    command._spark_events = MagicMock()

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(tls.TestLivySession.pi_result, result[1])
    command._spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                        session.id, command.guid)
    command._spark_events.emit_statement_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                                      session.id, command.guid,
                                                                                      0, True, "", "")


def test_execute_failure_emits_event():
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_json
    conf.override_all({
        "status_sleep_seconds": 0.01,
        "statement_sleep_seconds": 0.01
    })
    session = tls.TestLivySession._create_session(kind=kind, http_client=http_client)
    conf.load()
    session.start(create_sql_context=False)
    session.wait_for_idle = MagicMock(side_effect=ValueError("yo"))
    command = Command("command")
    command._spark_events = MagicMock()

    try:
        result = command.execute(session)
        assert False
    except ValueError:
        command._spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                            session.id, command.guid)
        command._spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                            session.id, command.guid,
                                                                                            -1, False, "ValueError", "yo")