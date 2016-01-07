from mock import MagicMock
from nose.tools import with_setup
import json

from remotespark.livyclientlib.sparkcontroller import SparkController

client_manager = None
client_factory = None
controller = None


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text

    def json(self):
        return json.loads(self._json_text)

    @property
    def status_code(self):
        return self._status_code


def _setup():
    global client_manager, client_factory, controller

    client_manager = MagicMock()
    client_factory = MagicMock()
    controller = SparkController()
    controller.client_manager = client_manager
    controller.client_factory = client_factory


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_add_session():
    name = "name"
    properties = {"kind": "spark"}
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    client_factory.create_session = MagicMock(return_value=session)
    client_factory.build_client = MagicMock(return_value=client)

    controller.add_session(name, connection_string, False, properties)

    client_factory.create_session.assert_called_once_with(connection_string, properties, "-1", False)
    client_factory.build_client.assert_called_once_with(session)
    client_manager.add_client.assert_called_once_with(name, client)
    session.start.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_add_session_skip():
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    client_factory.create_session = MagicMock(return_value=session)
    client_factory.build_client = MagicMock(return_value=client)

    client_manager.get_sessions_list.return_value = [name]
    controller.add_session(name, language, connection_string, True)

    assert client_factory.create_session.call_count == 0
    assert client_factory.build_client.call_count == 0
    assert client_manager.add_client.call_count == 0
    assert session.start.call_count == 0


@with_setup(_setup, _teardown)
def test_delete_session():
    name = "name"

    controller.delete_session_by_name(name)

    client_manager.delete_client.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_cleanup():
    controller.cleanup()
    client_manager.clean_up_all.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_run_cell():
    default_client = MagicMock()
    chosen_client = MagicMock()
    default_client.execute = chosen_client.execute = MagicMock(return_value=(True, ""))
    client_manager.get_any_client = MagicMock(return_value=default_client)
    client_manager.get_client = MagicMock(return_value=chosen_client)
    name = "session_name"
    cell = "cell code"

    controller.run_cell(cell, name)
    chosen_client.execute.assert_called_with(cell)

    controller.run_cell(cell, None)
    default_client.execute.assert_called_with(cell)

    controller.run_cell_sql(cell, name)
    chosen_client.execute_sql.assert_called_with(cell)

    controller.run_cell_sql(cell, None)
    default_client.execute_sql.assert_called_with(cell)

    controller.run_cell_hive(cell, name)
    chosen_client.execute_hive.assert_called_with(cell)

    controller.run_cell_hive(cell, None)
    default_client.execute_hive.assert_called_with(cell)


@with_setup(_setup, _teardown)
def test_get_client_keys():
    controller.get_client_keys()
    client_manager.get_sessions_list.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_get_all_sessions():
    http_client = MagicMock()
    http_client.get.return_value = DummyResponse(200, '{"from":0,"total":2,"sessions":[{"id":0,"state":"idle","kind":'
                                                      '"spark","log":[""]}, {"id":1,"state":"busy","kind":"spark","log"'
                                                      ':[""]}]}')
    client_factory.create_http_client.return_value = http_client

    sessions = controller.get_all_sessions_endpoint("conn_str")

    assert len(sessions) == 2


@with_setup(_setup, _teardown)
def test_cleanup_endpoint():
    s0 = MagicMock()
    s1 = MagicMock()
    controller.get_all_sessions_endpoint = MagicMock(return_value=[s0, s1])

    controller.cleanup_endpoint("conn_str")

    s0.delete.assert_called_once_with()
    s1.delete.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_session_by_id_existent():
    http_client = MagicMock()
    http_client.get.return_value = DummyResponse(200, '{"id":0,"state":"starting","kind":"spark","log":[]}')
    client_factory.create_http_client.return_value = http_client
    session = MagicMock()
    create_session_method = MagicMock(return_value=session)
    client_factory.create_session = create_session_method

    controller.delete_session_by_id("conn_str", "0")

    create_session_method.assert_called_once_with("conn_str", {"kind": "spark"}, "0", False)
    session.delete.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_session_by_id_non_existent():
    http_client = MagicMock()
    http_client.get.return_value = DummyResponse(404, '')
    client_factory.create_http_client.return_value = http_client
    session = MagicMock()
    create_session_method = MagicMock(return_value=session)
    client_factory.create_session = create_session_method

    controller.delete_session_by_id("conn_str", "0")

    assert len(create_session_method.mock_calls) == 0
    assert len(session.delete.mock_calls) == 0
