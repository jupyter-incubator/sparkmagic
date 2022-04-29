from mock import MagicMock
from nose.tools import assert_equals
from time import sleep

from sparkmagic.livyclientlib.livysession import _HeartbeatThread


def test_create_thread():
    session = MagicMock()
    refresh_seconds = 1
    retry_seconds = 2
    heartbeat_thread = _HeartbeatThread(session, refresh_seconds, retry_seconds)

    assert_equals(heartbeat_thread.livy_session, session)
    assert_equals(heartbeat_thread.refresh_seconds, refresh_seconds)
    assert_equals(heartbeat_thread.retry_seconds, retry_seconds)


def test_run_once():
    session = MagicMock()
    refresh_seconds = 0.1
    retry_seconds = 2
    heartbeat_thread = _HeartbeatThread(session, refresh_seconds, retry_seconds, 1)

    heartbeat_thread.start()
    sleep(0.15)
    heartbeat_thread.stop()

    session.refresh_status_and_info.assert_called_once_with()
    assert heartbeat_thread.livy_session is None


def test_run_stops():
    session = MagicMock()
    refresh_seconds = 0.01
    retry_seconds = 2
    heartbeat_thread = _HeartbeatThread(session, refresh_seconds, retry_seconds)

    heartbeat_thread.start()
    sleep(0.1)
    heartbeat_thread.stop()

    assert session.refresh_status_and_info.called
    assert heartbeat_thread.livy_session is None


def test_run_retries():
    msg = "oh noes!"
    session = MagicMock()
    session.refresh_status_and_info = MagicMock(side_effect=ValueError(msg))
    refresh_seconds = 0.1
    retry_seconds = 0.1
    heartbeat_thread = _HeartbeatThread(session, refresh_seconds, retry_seconds, 1)

    heartbeat_thread.start()
    sleep(0.15)
    heartbeat_thread.stop()

    session.refresh_status_and_info.assert_called_once_with()
    session.logger.error.assert_called_once_with(msg)
    assert heartbeat_thread.livy_session is None


def test_run_retries_stops():
    msg = "oh noes!"
    session = MagicMock()
    session.refresh_status_and_info = MagicMock(side_effect=ValueError(msg))
    refresh_seconds = 0.01
    retry_seconds = 0.01
    heartbeat_thread = _HeartbeatThread(session, refresh_seconds, retry_seconds)

    heartbeat_thread.start()
    sleep(0.1)
    heartbeat_thread.stop()

    assert session.refresh_status_and_info.called
    assert session.logger.error.called
    assert heartbeat_thread.livy_session is None
