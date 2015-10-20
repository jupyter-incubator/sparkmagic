from nose.tools import with_setup, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.log import Log
from remotespark.livyclientlib.sparkcontroller import SparkController


client_manager = None
client_factory = None
controller = None


def _setup():
    global client_manager, client_factory, controller

    client_manager = MagicMock()
    client_factory = MagicMock()
    controller = SparkController("normal")
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

    controller.add_endpoint(name, language, connection_string)

    client_factory.create_session.assert_called_once_with(language, connection_string, "-1", False)
    client_factory.build_client.assert_called_once_with(language, session)
    client_manager.add_client.assert_called_once_with(name, client)
    session.start.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_endpoint():
    name = "name"

    controller.delete_endpoint(name)

    client_manager.delete_client.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_log_mode():
    mode = "debug"

    controller.set_log_mode(mode)

    assert_equals(mode, Log.mode)
    assert_equals(mode, controller.get_log_mode())


@with_setup(_setup, _teardown)
def test_cleanup():
    controller.cleanup()
    client_manager.clean_up_all.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_run_cell():
    default_client = MagicMock()
    chosen_client = MagicMock()
    client_manager.get_any_client = MagicMock(return_value=default_client)
    client_manager.get_client = MagicMock(return_value=chosen_client)
    name = "endpoint_name"
    sql = False
    cell = "cell code"

    controller.run_cell(name, sql, cell)
    chosen_client.execute.assert_called_with(cell)

    controller.run_cell(None, sql, cell)
    default_client.execute.assert_called_with(cell)

    sql = True

    controller.run_cell(name, sql, cell)
    chosen_client.execute_sql.assert_called_with(cell)

    controller.run_cell(None, sql, cell)
    default_client.execute_sql.assert_called_with(cell)

@with_setup(_setup, _teardown)
def test_get_client_keys():
    controller.get_client_keys()
    client_manager.get_endpoints_list.assert_called_once_with()
