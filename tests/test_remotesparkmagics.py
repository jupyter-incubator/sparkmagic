from mock import MagicMock
from nose.tools import with_setup

import remotespark.utils.configuration as conf
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException
from remotespark.magics.remotesparkmagics import RemoteSparkMagics
from remotespark.utils.constants import Constants

magic = None
spark_controller = None
shell = None
ipython_display = None


def _setup():
    global magic, spark_controller, shell, ipython_display

    conf.override_all({})

    shell = MagicMock()
    ipython_display = MagicMock()
    magic = RemoteSparkMagics(shell=None)
    magic.shell = shell
    magic.ipython_display = ipython_display

    spark_controller = MagicMock()
    magic.spark_controller = spark_controller


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_info_command_parses():
    print_info_mock = MagicMock()
    magic._print_local_info = print_info_mock
    command = "info"

    magic.spark(command)

    print_info_mock.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_info_endpoint_command_parses():
    print_info_mock = MagicMock()
    magic._print_endpoint_info = print_info_mock
    command = "info conn_str"
    spark_controller.get_all_sessions_endpoint_info = MagicMock(return_value=None)

    magic.spark(command)

    print_info_mock.assert_called_once_with(None)


@with_setup(_setup, _teardown)
def test_add_sessions_command_parses():
    # Do not skip and python
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with(name, connection_string, False, {"kind": "pyspark"})

    # Skip and scala - upper case
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "name"
    language = "Scala"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string, "skip"])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with(name, connection_string, True, {"kind": "spark"})


@with_setup(_setup, _teardown)
def test_add_sessions_command_extra_properties():
    conf.override_all({})
    magic.spark("config {\"extra\": \"yes\"}")
    assert conf.session_configs() == {"extra": "yes"}

    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "name"
    language = "scala"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with(name, connection_string, False, {"kind": "spark", "extra": "yes"})
    conf.load()


@with_setup(_setup, _teardown)
def test_delete_sessions_command_parses():
    mock_method = MagicMock()
    spark_controller.delete_session_by_name = mock_method
    command = "delete"
    name = "name"
    line = " ".join([command, name])

    magic.spark(line)

    mock_method.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_delete_sessions_command_parses():
    mock_method = MagicMock()
    spark_controller.delete_session_by_id = mock_method
    line = "delete conn_str 7"

    magic.spark(line)

    mock_method.assert_called_once_with("conn_str", "7")


@with_setup(_setup, _teardown)
def test_cleanup_command_parses():
    mock_method = MagicMock()
    spark_controller.cleanup = mock_method
    line = "cleanup"

    magic.spark(line)

    mock_method.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_cleanup_endpoint_command_parses():
    mock_method = MagicMock()
    spark_controller.cleanup_endpoint = mock_method
    line = "cleanup conn_str"

    magic.spark(line)

    mock_method.assert_called_once_with("conn_str")


@with_setup(_setup, _teardown)
def test_bad_command_writes_error():
    line = "bad_command"
    usage = "Please look at usage of %spark by executing `%spark?`."

    magic.spark(line)

    ipython_display.send_error.assert_called_once_with("Subcommand '{}' not found. {}".format(line, usage))


@with_setup(_setup, _teardown)
def test_run_cell_command_parses():
    run_cell_method = MagicMock()
    result_value = ""
    run_cell_method.return_value = (True, result_value)
    spark_controller.run_cell = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is None
    ipython_display.write.assert_called_once_with(result_value)


@with_setup(_setup, _teardown)
def test_run_cell_command_writes_to_err():
    run_cell_method = MagicMock()
    result_value = ""
    run_cell_method.return_value = (False, result_value)
    spark_controller.run_cell = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(result_value)


@with_setup(_setup, _teardown)
def test_run_sql_command_parses():
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell_sql = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    line = " ".join([command, name, context, context_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is not None


@with_setup(_setup, _teardown)
def test_run_hive_command_parses():
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell_hive = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "hive"
    line = " ".join([command, name, context, context_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is not None


@with_setup(_setup, _teardown)
def test_run_sql_command_returns_none_when_exception():
    error_message = "error"
    run_cell_method = MagicMock(side_effect=DataFrameParseException(error_message))
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell_sql = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    line = " ".join([command, name, context, context_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(error_message)


@with_setup(_setup, _teardown)
def test_run_hive_command_returns_none_when_exception():
    error_message = "error"
    run_cell_method = MagicMock(side_effect=DataFrameParseException(error_message))
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell_hive = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "hive"
    line = " ".join([command, name, context, context_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(error_message)


@with_setup(_setup, _teardown)
def test_run_sql_command_stores_variable_in_user_ns():
    shell.user_ns = user_ns = dict()
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell_sql = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    output = "-o"
    output_name = "my_var"
    line = " ".join([command, name, context, context_name, output, output_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)
    assert result is not None
    assert result is user_ns[output_name]


def test_get_livy_kind_covers_all_langs():
    for lang in Constants.lang_supported:
        RemoteSparkMagics._get_livy_kind(lang)


@with_setup(_setup, _teardown)
def test_logs_subcommand():
    get_logs_method = MagicMock()
    result_value = ""
    get_logs_method.return_value = (True, result_value)
    spark_controller.get_logs = get_logs_method

    command = "logs -s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    # Could get results
    result = magic.spark(line, cell)

    get_logs_method.assert_called_once_with(name)
    assert result is None
    ipython_display.write.assert_called_once_with(result_value)

    # Could not get results
    get_logs_method.reset_mock()
    get_logs_method.return_value = (False, result_value)

    result = magic.spark(line, cell)

    get_logs_method.assert_called_once_with(name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(result_value)
