import sys
from base64 import b64encode
from contextlib import contextmanager
from mock import MagicMock
from nose.tools import assert_equals, with_setup

from IPython.display import Image

import sparkmagic.livyclientlib.exceptions
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import (
    SESSION_KIND_SPARK,
    MIMETYPE_IMAGE_PNG,
    MIMETYPE_TEXT_HTML,
    MIMETYPE_TEXT_PLAIN,
    COMMAND_INTERRUPTED_MSG,
)
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.livysession import LivySession
from sparkmagic.livyclientlib.exceptions import SparkStatementCancelledException
from . import test_livysession as tls


if sys.version_info[0] == 2:
    from StringIO import StringIO
elif sys.version_info[0] == 3:
    from io import StringIO
else:
    assert False


def _setup():
    conf.override_all({})


def _create_session(
    kind=SESSION_KIND_SPARK, session_id=-1, http_client=None, spark_events=None
):
    if http_client is None:
        http_client = MagicMock()
    if spark_events is None:
        spark_events = MagicMock()
    ipython_display = MagicMock()
    session = LivySession(
        http_client,
        {"kind": kind, "heartbeatTimeoutInSecond": 60},
        ipython_display,
        session_id,
        spark_events,
    )
    return session


@contextmanager
def _capture_stderr():
    try:
        sys.stderr = StringIO()
        yield sys.stderr
    finally:
        sys.stderr = sys.__stderr__


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
    assert_equals(MIMETYPE_TEXT_PLAIN, result[2])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid
    )
    spark_events.emit_statement_execution_end_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid, 0, True, "", ""
    )

    # Now try with PNG result:
    http_client.get_statement.return_value = {
        "id": 0,
        "state": "available",
        "output": {
            "status": "ok",
            "execution_count": 0,
            "data": {"text/plain": "", "image/png": b64encode(b"hello")},
        },
    }
    result = command.execute(session)
    assert result[0]
    assert isinstance(result[1], Image)
    assert result[1].data == b"hello"
    assert_equals(MIMETYPE_IMAGE_PNG, result[2])

    # Now try with HTML result:
    http_client.get_statement.return_value = {
        "id": 0,
        "state": "available",
        "output": {
            "status": "ok",
            "execution_count": 0,
            "data": {"text/html": "<p>out</p>"},
        },
    }
    result = command.execute(session)
    assert result[0]
    assert_equals("<p>out</p>", result[1])
    assert_equals(MIMETYPE_TEXT_HTML, result[2])


@with_setup(_setup)
def test_execute_waiting():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.side_effect = [
        tls.TestLivySession.waiting_statement_json,
        tls.TestLivySession.waiting_statement_json,
        tls.TestLivySession.ready_statement_json,
        tls.TestLivySession.ready_statement_json,
    ]
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals(tls.TestLivySession.pi_result, result[1])
    assert_equals(MIMETYPE_TEXT_PLAIN, result[2])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid
    )
    spark_events.emit_statement_execution_end_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid, 0, True, "", ""
    )


@with_setup(_setup)
def test_execute_null_ouput():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.post_session.return_value = tls.TestLivySession.session_create_json
    http_client.post_statement.return_value = tls.TestLivySession.post_statement_json
    http_client.get_session.return_value = tls.TestLivySession.ready_sessions_json
    http_client.get_statement.return_value = (
        tls.TestLivySession.ready_statement_null_output_json
    )
    session = _create_session(kind=kind, http_client=http_client)
    session.start()
    command = Command("command", spark_events=spark_events)

    result = command.execute(session)

    http_client.post_statement.assert_called_with(0, {"code": command.code})
    http_client.get_statement.assert_called_with(0, 0)
    assert result[0]
    assert_equals("", result[1])
    assert_equals(MIMETYPE_TEXT_PLAIN, result[2])
    spark_events.emit_statement_execution_start_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid
    )
    spark_events.emit_statement_execution_end_event.assert_called_once_with(
        session.guid, session.kind, session.id, command.guid, 0, True, "", ""
    )


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
        spark_events.emit_statement_execution_start_event.assert_called_with(
            session.guid, session.kind, session.id, command.guid
        )
        spark_events.emit_statement_execution_end_event.assert_called_once_with(
            session.guid,
            session.kind,
            session.id,
            command.guid,
            -1,
            False,
            "ValueError",
            "yo",
        )
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

    http_client.post_statement.side_effect = KeyError("Something bad happened here")
    try:
        result = command.execute(session)
        assert False
    except KeyError as e:
        spark_events.emit_statement_execution_start_event.assert_called_once_with(
            session.guid, session.kind, session.id, command.guid
        )
        spark_events.emit_statement_execution_end_event._assert_called_once_with(
            session.guid,
            session.kind,
            session.id,
            command.guid,
            -1,
            False,
            "KeyError",
            "Something bad happened here",
        )
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
    command._get_statement_output = MagicMock(side_effect=AttributeError("OHHHH"))

    try:
        result = command.execute(session)
        assert False
    except AttributeError as e:
        spark_events.emit_statement_execution_start_event.assert_called_once_with(
            session.guid, session.kind, session.id, command.guid
        )
        spark_events.emit_statement_execution_end_event._assert_called_once_with(
            session.guid,
            session.kind,
            session.id,
            command.guid,
            -1,
            False,
            "AttributeError",
            "OHHHH",
        )
        assert_equals(e, command._get_statement_output.side_effect)


@with_setup(_setup)
def test_execute_interrupted():
    spark_events = MagicMock()
    kind = SESSION_KIND_SPARK
    http_client = MagicMock()
    http_client.get_statement.return_value = tls.TestLivySession.ready_statement_json
    session = _create_session(kind=kind, http_client=http_client)
    session.wait_for_idle = MagicMock()
    session.start()
    session.wait_for_idle = MagicMock()
    command = Command("command", spark_events=spark_events)

    mock_ipython = MagicMock()
    mock_get_ipython = lambda: mock_ipython
    mock_ipython._showtraceback = mock_show_tb = MagicMock()
    sparkmagic.livyclientlib.exceptions.get_ipython = mock_get_ipython
    http_client.post_statement.side_effect = KeyboardInterrupt("")
    try:
        result = command.execute(session)
        assert False
    except KeyboardInterrupt as e:
        spark_events.emit_statement_execution_start_event.assert_called_once_with(
            session.guid, session.kind, session.id, command.guid
        )
        spark_events.emit_statement_execution_end_event._assert_called_once_with(
            session.guid,
            session.kind,
            session.id,
            command.guid,
            -1,
            False,
            "KeyboardInterrupt",
            "",
        )
        assert isinstance(e, SparkStatementCancelledException)
        assert_equals(str(e), COMMAND_INTERRUPTED_MSG)

        # Test patching _showtraceback()
        assert mock_ipython._showtraceback is SparkStatementCancelledException._show_tb

        with _capture_stderr() as stderr:
            mock_ipython._showtraceback(KeyError, "Dummy KeyError", MagicMock())
            mock_show_tb.assert_called_once()
            assert not stderr.getvalue()

        with _capture_stderr() as stderr:
            mock_ipython._showtraceback(
                SparkStatementCancelledException, COMMAND_INTERRUPTED_MSG, MagicMock()
            )
            mock_show_tb.assert_called_once()  # still once
            assert_equals(stderr.getvalue().strip(), COMMAND_INTERRUPTED_MSG)

    except:
        assert False
    else:
        assert False
