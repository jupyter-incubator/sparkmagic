from nose.tools import raises, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.clientmanager import ClientManager


@raises(ValueError)
def test_get_client_throws_when_client_not_exists():
    manager = ClientManager()

    manager.get_client("name")


def test_get_client():
    client = MagicMock()
    manager = ClientManager()

    manager.add_client("name", client)

    assert_equals(client, manager.get_client("name"))


@raises(ValueError)
def test_delete_client():
    client = MagicMock()
    manager = ClientManager()

    manager.add_client("name", client)
    manager.delete_client("name")

    manager.get_client("name")


@raises(ValueError)
def test_delete_client_throws_when_client_not_exists():
    manager = ClientManager()

    manager.delete_client("name")


@raises(ValueError)
def test_add_client_throws_when_client_exists():
    client = MagicMock()
    manager = ClientManager()

    manager.add_client("name", client)
    manager.add_client("name", client)


def test_client_names_returned():
    client = MagicMock()
    manager = ClientManager()

    manager.add_client("name0", client)
    manager.add_client("name1", client)

    assert_equals({"name0", "name1"}, set(manager.get_endpoints_list()))


def test_get_any_client():
    client = MagicMock()
    manager = ClientManager()

    manager.add_client("name", client)

    assert_equals(client, manager.get_any_client())


@raises(AssertionError)
def test_get_any_client_raises_exception_with_no_client():
    manager = ClientManager()

    manager.get_any_client()


@raises(AssertionError)
def test_get_any_client_raises_exception_with_two_clients():
    client = MagicMock()
    manager = ClientManager()
    manager.add_client("name0", client)
    manager.add_client("name1", client)

    manager.get_any_client()


def test_clean_up():
    client0 = MagicMock()
    client1 = MagicMock()
    manager = ClientManager()
    manager.add_client("name0", client0)
    manager.add_client("name1", client1)

    manager.clean_up_all()

    client0.close_sessions.assert_called_once_with()
    client1.close_sessions.assert_called_once_with()
