from mock import MagicMock, patch
import remotespark.utils.constants as constants
from nose.tools import with_setup
import json

from remotespark.livyclientlib.sparkcontroller import SparkController

client_manager = None
controller = None
ipython_display = None


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
    global client_manager, controller, ipython_display

    client_manager = MagicMock()
    ipython_display = MagicMock()
    controller = SparkController(ipython_display)
    controller.client_manager = client_manager

def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_add_session():
    name = "name"
    properties = {"kind": "spark"}
    connection_string = "url=http://location:port;username=name;password=word"
    client = MagicMock()
    session = MagicMock()
    session.kind = constants.SESSION_KIND_SPARK
    session.id = 0
    controller._create_livy_session = MagicMock(return_value=session)
    controller._create_livy_client = MagicMock(return_value=client)

    controller.add_session(name, connection_string, False, properties)

    controller._create_livy_session.assert_called_once_with(connection_string, properties, ipython_display)
    controller.client_manager.add_client.assert_called_once_with(name, client)
    client.start.assert_called_once_with()
    session.start.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_add_session_skip():
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    controller._create_livy_session = MagicMock(return_value=session)
    controller._http_client_from_connection_string = MagicMock(return_value=client)

    client_manager.get_sessions_list.return_value = [name]
    controller.add_session(name, language, connection_string, True)

    assert controller._create_livy_session.create_session.call_count == 0
    assert controller._http_client_from_connection_string.build_client.call_count == 0
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
    controller._http_client_from_connection_string = MagicMock(return_value=http_client)
    controller._create_livy_session = MagicMock()

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
    controller._http_client_from_connection_string = MagicMock(return_value=http_client)
    session = MagicMock()
    controller._create_livy_session = MagicMock(return_value=session)

    controller.delete_session_by_id("conn_str", "0")

    controller._create_livy_session.assert_called_once_with("conn_str", {"kind": "spark"}, ipython_display, "0", False)
    session.delete.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_session_by_id_non_existent():
    http_client = MagicMock()
    http_client.get.return_value = DummyResponse(404, '')
    controller._http_client_from_connection_string = MagicMock(return_value=http_client)
    session = MagicMock()
    controller._create_livy_session = MagicMock(return_value=session)

    controller.delete_session_by_id("conn_str", "0")

    assert len(controller._create_livy_session.mock_calls) == 0
    assert len(session.delete.mock_calls) == 0


@with_setup(_setup, _teardown)
def test_get_logs():
    chosen_client = MagicMock()
    controller.get_client_by_name_or_default = MagicMock(return_value=chosen_client)

    controller.get_logs()

    chosen_client.get_logs.assert_called_with()


@with_setup(_setup, _teardown)
def test_get_session_id_for_client():
    assert controller.get_session_id_for_client("name") is not None
    client_manager.get_session_id_for_client.assert_called_once_with("name")
