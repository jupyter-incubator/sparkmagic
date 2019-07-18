from base64 import b64encode
from mock import MagicMock
from nose.tools import assert_equals, with_setup

from IPython.display import Image

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import SESSION_KIND_SPARK
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.livysession import LivySession
from . import test_livysession as tls


def _setup():
    conf.override_all({})
    

def _create_session(kind=SESSION_KIND_SPARK, session_id=-1,
                    http_client=None, spark_events=None):
    if http_client is None:
        http_client = MagicMock()
    if spark_events is None:
        spark_events = MagicMock()
    ipython_display = MagicMock()
    session = LivySession(http_client, {"kind": kind, "heartbeatTimeoutInSecond": 60},
                          ipython_display, session_id, spark_events)
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
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(tls.TestLivySession.pi_result, result[1])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                                        session.id, command.guid)
    spark_events.emit_statement_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                                      session.id, command.guid,
                                                                                      0, True, "", "")

    # Now try with PNG result:
    http_client.get_statement.return_value = {"id":0,"state":"available","output":{"status":"ok", "execution_count":0,"data":{"text/plain":"", "image/png": b64encode(b"hello")}}}
    result = command.execute(session)
    assert result[0]
    assert isinstance(result[1], Image)
    assert result[1].data == b"hello"


@with_setup(_setup)
def test_execute_waiting():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.side_effect = [tls.TestLivySession.waiting_statement_json, tls.TestLivySession.waiting_statement_json, tls.TestLivySession.ready_statement_json, tls.TestLivySession.ready_statement_json]
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(tls.TestLivySession.pi_result, result[1])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                                        session.id, command.guid)
    spark_events.emit_statement_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                                      session.id, command.guid,
                                                                                      0, True, "", "")


@with_setup(_setup)
def test_execute_null_ouput():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_null_output_json
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(u"", result[1])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                                        session.id, command.guid)
    spark_events.emit_statement_execution_end_event.assert_called_once_with(session.guid, session.kind,
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
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    session.wait_for_idle = MagicMock(side_effect=ValueError("yo"))
    command = Command("command", spark_events=spark_events)

    try:
        result = command.execute(session)
        assert False
    except ValueError as e:
        spark_events.emit_statement_execution_start_event.assert_called_with(session.guid, session.kind,
                                                                                  session.id, command.guid)
        spark_events.emit_statement_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "ValueError", "yo")
        assert_equals(e, session.wait_for_idle.side_effect)


@with_setup(_setup)
def test_execute_failure_post_statement_emits_event():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_json
    session = _create_session(kind=kind, http_client=http_client)
    session.wait_for_idle = MagicMock()
    session.start()
    session.wait_for_idle = MagicMock()
    command = Command("command", spark_events=spark_events)

    http_client.post_statement.side_effect = KeyError('Something bad happened here')
    try:
        result = command.execute(session)
        assert False
    except KeyError as e:
        spark_events.emit_statement_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid)
        spark_events.emit_statement_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "KeyError",
                                                                                   "Something bad happened here")
        assert_equals(e, http_client.post_statement.side_effect)


@with_setup(_setup)
def test_execute_failure_get_statement_output_emits_event():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_json

    session = _create_session(kind=kind, http_client=http_client)
    session.wait_for_idle = MagicMock()
    session.start()
    session.wait_for_idle = MagicMock()
    command = Command("command", spark_events=spark_events)
    command._get_statement_output = MagicMock(side_effect=AttributeError('OHHHH'))

    try:
        result = command.execute(session)
        assert False
    except AttributeError as e:
        spark_events.emit_statement_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid)
        spark_events.emit_statement_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                                   session.id, command.guid,
                                                                                   -1, False, "AttributeError",
                                                                                   "OHHHH")
        assert_equals(e, command._get_statement_output.side_effect)
