import atexit
from mock import MagicMock, PropertyMock
from nose.tools import raises, assert_equals

import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import SessionManagementException
from sparkmagic.livyclientlib.sessionmanager import SessionManager


@raises(SessionManagementException)
def test_get_client_throws_when_client_not_exists():
    manager = get_session_manager()

    manager.get_session("name")


def test_get_client():
    client = MagicMock()
    manager = get_session_manager()

    manager.add_session("name", client)

    assert_equals(client, manager.get_session("name"))


@raises(SessionManagementException)
def test_delete_client():
    client = MagicMock()
    manager = get_session_manager()

    manager.add_session("name", client)
    manager.delete_client("name")

    manager.get_session("name")


@raises(SessionManagementException)
def test_delete_client_throws_when_client_not_exists():
    manager = get_session_manager()

    manager.delete_client("name")


@raises(SessionManagementException)
def test_add_client_throws_when_client_exists():
    client = MagicMock()
    manager = get_session_manager()

    manager.add_session("name", client)
    manager.add_session("name", client)


def test_client_names_returned():
    client = MagicMock()
    manager = get_session_manager()

    manager.add_session("name0", client)
    manager.add_session("name1", client)

    assert_equals({"name0", "name1"}, set(manager.get_sessions_list()))


def test_get_any_client():
    client = MagicMock()
    manager = get_session_manager()

    manager.add_session("name", client)

    assert_equals(client, manager.get_any_session())


@raises(SessionManagementException)
def test_get_any_client_raises_exception_with_no_client():
    manager = get_session_manager()

    manager.get_any_session()


@raises(SessionManagementException)
def test_get_any_client_raises_exception_with_two_clients():
    client = MagicMock()
    manager = get_session_manager()
    manager.add_session("name0", client)
    manager.add_session("name1", client)

    manager.get_any_session()


def test_clean_up():
    client0 = MagicMock()
    client1 = MagicMock()
    manager = get_session_manager()
    manager.add_session("name0", client0)
    manager.add_session("name1", client1)

    manager.clean_up_all()

    client0.delete.assert_called_once_with()
    client1.delete.assert_called_once_with()


def test_cleanup_all_sessions_on_exit():
    conf.override(conf.cleanup_all_sessions_on_exit.__name__, True)
    client0 = MagicMock()
    client1 = MagicMock()
    manager = get_session_manager()
    manager.add_session("name0", client0)
    manager.add_session("name1", client1)

    atexit._run_exitfuncs()

    client0.delete.assert_called_once_with()
    client1.delete.assert_called_once_with()
    manager.ipython_display.writeln.assert_called_once_with(
        "Cleaning up livy sessions on exit is enabled"
    )


def test_cleanup_all_sessions_on_exit_fails():
    """
    Cleanup on exit is best effort only. When cleanup fails, exception is caught and error is logged.
    """
    conf.override(conf.cleanup_all_sessions_on_exit.__name__, True)
    client0 = MagicMock()
    client1 = MagicMock()
    client0.delete.side_effect = Exception("Mocked exception for client1.delete")
    manager = get_session_manager()
    manager.add_session("name0", client0)
    manager.add_session("name1", client1)

    atexit._run_exitfuncs()

    client0.delete.assert_called_once_with()
    client1.delete.assert_not_called()


def test_get_session_id_for_client():
    manager = get_session_manager()
    manager.get_sessions_list = MagicMock(return_value=["name"])
    manager._sessions["name"] = MagicMock()

    id = manager.get_session_id_for_client("name")

    assert id is not None


def test_get_session_name_by_id_endpoint():
    manager = get_session_manager()
    id_to_search = "0"
    endpoint_to_search = "endpoint"
    name_to_search = "name"

    name = manager.get_session_name_by_id_endpoint(id_to_search, endpoint_to_search)
    assert_equals(None, name)

    session = MagicMock()
    type(session).id = PropertyMock(return_value=int(id_to_search))
    session.endpoint = endpoint_to_search

    manager.add_session(name_to_search, session)
    name = manager.get_session_name_by_id_endpoint(id_to_search, endpoint_to_search)
    assert_equals(name_to_search, name)


def test_get_session_id_for_client_not_there():
    manager = get_session_manager()
    manager.get_sessions_list = MagicMock(return_value=[])

    id = manager.get_session_id_for_client("name")

    assert id is None


def get_session_manager():
    ipython_display = MagicMock()
    return SessionManager(ipython_display)
