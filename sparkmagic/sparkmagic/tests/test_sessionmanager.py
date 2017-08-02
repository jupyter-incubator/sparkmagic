from mock import MagicMock, PropertyMock
from nose.tools import raises, assert_equals

from sparkmagic.livyclientlib.exceptions import SessionManagementException
from sparkmagic.livyclientlib.sessionmanager import SessionManager


@raises(SessionManagementException)
def test_get_client_throws_when_client_not_exists():
    manager = SessionManager()

    manager.get_session("name")


def test_get_client():
    client = MagicMock()
    manager = SessionManager()

    manager.add_session("name", client)

    assert_equals(client, manager.get_session("name"))


@raises(SessionManagementException)
def test_delete_client():
    client = MagicMock()
    manager = SessionManager()

    manager.add_session("name", client)
    manager.delete_client("name")

    manager.get_session("name")


@raises(SessionManagementException)
def test_delete_client_throws_when_client_not_exists():
    manager = SessionManager()

    manager.delete_client("name")


@raises(SessionManagementException)
def test_add_client_throws_when_client_exists():
    client = MagicMock()
    manager = SessionManager()

    manager.add_session("name", client)
    manager.add_session("name", client)


def test_client_names_returned():
    client = MagicMock()
    manager = SessionManager()

    manager.add_session("name0", client)
    manager.add_session("name1", client)

    assert_equals({"name0", "name1"}, set(manager.get_sessions_list()))


def test_get_any_client():
    client = MagicMock()
    manager = SessionManager()

    manager.add_session("name", client)

    assert_equals(client, manager.get_any_session())


@raises(SessionManagementException)
def test_get_any_client_raises_exception_with_no_client():
    manager = SessionManager()

    manager.get_any_session()


@raises(SessionManagementException)
def test_get_any_client_raises_exception_with_two_clients():
    client = MagicMock()
    manager = SessionManager()
    manager.add_session("name0", client)
    manager.add_session("name1", client)

    manager.get_any_session()


def test_clean_up():
    client0 = MagicMock()
    client1 = MagicMock()
    manager = SessionManager()
    manager.add_session("name0", client0)
    manager.add_session("name1", client1)

    manager.clean_up_all()

    client0.delete.assert_called_once_with()
    client1.delete.assert_called_once_with()


def test_get_session_id_for_client():
    manager = SessionManager()
    manager.get_sessions_list = MagicMock(return_value=["name"])
    manager._sessions["name"] = MagicMock()

    id = manager.get_session_id_for_client("name")

    assert id is not None


def test_get_session_name_by_id_endpoint():
    manager = SessionManager()
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
    manager = SessionManager()
    manager.get_sessions_list = MagicMock(return_value=[])

    id = manager.get_session_id_for_client("name")

    assert id is None
