from mock import MagicMock
from nose.tools import assert_equals, with_setup

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import SESSION_KIND_SPARK
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.livysession import LivySession
from . import test_livysession as tls


def _setup():
    conf.override_all({})
    

def _create_session(kind=SESSION_KIND_SPARK, session_id=-1,
                    sql_created=False, http_client=None, spark_events=None):
    if http_client is None:
        http_client = MagicMock()
    if spark_events is None:
        spark_events = MagicMock()
    ipython_display = MagicMock()
    session = LivySession(http_client, {"kind": kind}, ipython_display, session_id, sql_created, spark_events)
    return session


@with_setup(_setup)
def test_execute():
    spark_events = MagicMock()
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
    session = _create_session(kind=kind, http_client=http_client)
    conf.override_all({})
    session.start(create_sql_context=False)
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(tls.TestLivySession.pi_result, result[1])
    spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                        session.id, command.guid)
    spark_events.emit_statement_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                                      session.id, command.guid,
                                                                                      0, True, "", "")


@with_setup(_setup)
def test_execute_failure_wait_for_session_emits_event():
    spark_events = MagicMock()
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
    session = _create_session(kind=kind, http_client=http_client)
    conf.override_all({})
    session.start(create_sql_context=False)
    session.wait_for_idle = MagicMock(side_effect=ValueError("yo"))
    command = Command("command", spark_events=spark_events)

    try:
        result = command.execute(session)
        assert False
    except ValueError as e:
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid)
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "ValueError", "yo")
        assert_equals(e, session.wait_for_idle.side_effect)


@with_setup(_setup)
def test_execute_failure_post_statement_emits_event():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_statement.side_effect = KeyError('Something bad happened here')
    conf.override_all({
        "status_sleep_seconds": 0.01,
        "statement_sleep_seconds": 0.01
    })
    session = _create_session(kind=kind, http_client=http_client)
    session.wait_for_idle = MagicMock()
    conf.override_all({})
    session.start(create_sql_context=False)
    session.wait_for_idle = MagicMock()
    command = Command("command", spark_events=spark_events)

    try:
        result = command.execute(session)
        assert False
    except KeyError as e:
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid)
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "KeyError",
                                                                                   "Something bad happened here")
        assert_equals(e, http_client.post_statement.side_effect)


@with_setup(_setup)
def test_execute_failure_get_statement_output_emits_event():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    conf.override_all({
        "status_sleep_seconds": 0.01,
        "statement_sleep_seconds": 0.01
    })
    session = _create_session(kind=kind, http_client=http_client)
    session.wait_for_idle = MagicMock()
    conf.override_all({})
    session.start(create_sql_context=False)
    session.wait_for_idle = MagicMock()
    command = Command("command", spark_events=spark_events)
    command._get_statement_output = MagicMock(side_effect=AttributeError('OHHHH'))

    try:
        result = command.execute(session)
        assert False
    except AttributeError as e:
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid)
        spark_events.emit_statement_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "AttributeError",
                                                                                   "OHHHH")
        assert_equals(e, command._get_statement_output.side_effect)
