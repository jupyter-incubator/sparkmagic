from nose.tools import with_setup, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.constants import Constants
from remotespark.livyclientlib.sparkcontroller import SparkController


client_manager = None
client_factory = None
controller = None


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
def test_add_endpoint():
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    client_factory.create_session = MagicMock(return_value=session)
    client_factory.build_client = MagicMock(return_value=client)

    controller.add_endpoint(name, language, connection_string, False)

    client_factory.create_session.assert_called_once_with(language, connection_string, "-1", False)
    client_factory.build_client.assert_called_once_with(language, session)
    client_manager.add_client.assert_called_once_with(name, client)
    session.start.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_add_endpoint_skip():
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    client_factory.create_session = MagicMock(return_value=session)
    client_factory.build_client = MagicMock(return_value=client)

    client_manager.get_endpoints_list.return_value = [name]
    controller.add_endpoint(name, language, connection_string, True)

    assert client_factory.create_session.call_count == 0
    assert client_factory.build_client.call_count == 0
    assert client_manager.add_client.call_count == 0
    assert session.start.call_count == 0


@with_setup(_setup, _teardown)
def test_delete_endpoint():
    name = "name"

    controller.delete_endpoint(name)

    client_manager.delete_client.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_cleanup():
    controller.cleanup()
    client_manager.clean_up_all.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_run_cell():
    default_client = MagicMock()
    chosen_client = MagicMock()
    default_client.execute = chosen_client.execute = MagicMock(return_value=(True,""))
    client_manager.get_any_client = MagicMock(return_value=default_client)
    client_manager.get_client = MagicMock(return_value=chosen_client)
    name = "endpoint_name"
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
    client_manager.get_endpoints_list.assert_called_once_with()
