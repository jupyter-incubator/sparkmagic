from mock import MagicMock
from nose.tools import with_setup
import hdijupyterutils.configuration as conf
from hdijupyterutils.constants import EXPECTED_ERROR_MSG

from sparkmagic.magics.remotesparkmagics import RemoteSparkMagics
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import *
from sparkmagic.livyclientlib.sqlquery import SQLQuery


magic = None
spark_controller = None
shell = None
ipython_display = None


def _setup():
    global magic, spark_controller, shell, ipython_display

    conf.override_all({})

    magic = RemoteSparkMagics(shell=None, widget=MagicMock())
    magic.shell = shell = MagicMock()
    magic.ipython_display = ipython_display = MagicMock()
    magic.spark_controller = spark_controller = MagicMock()


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
    magic.print_endpoint_info = print_info_mock
    command = "info -u http://microsoft.com"
    spark_controller.get_all_sessions_endpoint_info = MagicMock(return_value=None)

    magic.spark(command)

    print_info_mock.assert_called_once_with(None)


@with_setup(_setup, _teardown)
def test_info_command_exception():
    print_info_mock = MagicMock(side_effect=LivyClientTimeoutException('OHHHHHOHOHOHO'))
    magic._print_local_info = print_info_mock
    command = "info"

    magic.spark(command)

    print_info_mock.assert_called_once_with()
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(print_info_mock.side_effect))


@with_setup(_setup, _teardown)
def test_add_sessions_command_parses():
    # Do not skip and python
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "-s name"
    language = "-l python"
    connection_string = "-u http://url.com -a sdf -p w"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with("name", Endpoint("http://url.com", "sdf", "w"),
                                              False, {"kind": "pyspark"})

    # Skip and scala - upper case
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "-s name"
    language = "-l scala"
    connection_string = "--url http://location:port"
    line = " ".join([command, name, language, connection_string, "-k"])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with("name", Endpoint("http://location:port"),
                                              True, {"kind": "spark"})


@with_setup(_setup, _teardown)
def test_add_sessions_command_exception():
    # Do not skip and python
    add_sessions_mock = MagicMock(side_effect=BadUserDataException('hehe'))
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "-s name"
    language = "-l python"
    connection_string = "-u http://url.com -a sdf -p w"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with("name", Endpoint("http://url.com", "sdf", "w"),
                                              False, {"kind": "pyspark"})
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(add_sessions_mock.side_effect))


@with_setup(_setup, _teardown)
def test_add_sessions_command_extra_properties():
    conf.override_all({})
    magic.spark("config", "{\"extra\": \"yes\"}")
    assert conf.session_configs() == {"extra": "yes"}

    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "-s name"
    language = "-l scala"
    connection_string = "-u http://livyendpoint.com"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with("name", Endpoint("http://livyendpoint.com"),
                                              False, {"kind": "spark", "extra": "yes"})
    conf.load()


@with_setup(_setup, _teardown)
def test_delete_sessions_command_parses():
    mock_method = MagicMock()
    spark_controller.delete_session_by_name = mock_method
    command = "delete -s name"
    magic.spark(command)
    mock_method.assert_called_once_with("name")

    command = "delete -u URL -a username -p password -i 4"
    mock_method = MagicMock()
    spark_controller.delete_session_by_id = mock_method
    magic.spark(command)
    mock_method.assert_called_once_with(Endpoint("URL", "username", "password"), 4)


@with_setup(_setup, _teardown)
def test_delete_sessions_command_exception():
    mock_method = MagicMock(side_effect=LivyUnexpectedStatusException('FEEEEEELINGS'))
    spark_controller.delete_session_by_name = mock_method
    command = "delete -s name"
    magic.spark(command)
    mock_method.assert_called_once_with("name")
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(mock_method.side_effect))


@with_setup(_setup, _teardown)
def test_cleanup_command_parses():
    mock_method = MagicMock()
    spark_controller.cleanup = mock_method
    line = "cleanup"

    magic.spark(line)

    mock_method.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_cleanup_command_exception():
    mock_method = MagicMock(side_effect=SessionManagementException('Livy did something VERY BAD'))
    spark_controller.cleanup = mock_method
    line = "cleanup"

    magic.spark(line)
    mock_method.assert_called_once_with()
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(mock_method.side_effect))


@with_setup(_setup, _teardown)
def test_cleanup_endpoint_command_parses():
    mock_method = MagicMock()
    spark_controller.cleanup_endpoint = mock_method
    line = "cleanup -u endp"

    magic.spark(line)

    mock_method.assert_called_once_with(Endpoint("endp"))

    line = "cleanup -u endp -a user -p passw"
    magic.spark(line)
    mock_method.assert_called_with(Endpoint("endp", "user", "passw"))


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
    spark_controller.run_command = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(Command(cell), name)
    assert result is None
    ipython_display.write.assert_called_once_with(result_value)


@with_setup(_setup, _teardown)
def test_run_cell_command_writes_to_err():
    run_cell_method = MagicMock()
    result_value = ""
    run_cell_method.return_value = (False, result_value)
    spark_controller.run_command = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(Command(cell), name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(result_value)


@with_setup(_setup, _teardown)
def test_run_cell_command_exception():
    run_cell_method = MagicMock()
    run_cell_method.side_effect = HttpClientException('meh')
    spark_controller.run_command = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(Command(cell), name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(run_cell_method.side_effect))


@with_setup(_setup, _teardown)
def test_run_sql_command_parses():
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_sqlquery = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    meth = "-m"
    method_name = "sample"
    line = " ".join([command, name, context, context_name, meth, method_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(SQLQuery(cell, samplemethod=method_name), name)
    assert result is not None


@with_setup(_setup, _teardown)
def test_run_sql_command_exception():
    run_cell_method = MagicMock()
    run_cell_method.side_effect = LivyUnexpectedStatusException('WOW')
    spark_controller.run_sqlquery = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    meth = "-m"
    method_name = "sample"
    line = " ".join([command, name, context, context_name, meth, method_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(SQLQuery(cell, samplemethod=method_name), name)
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(run_cell_method.side_effect))


@with_setup(_setup, _teardown)
def test_run_sql_command_knows_how_to_be_quiet():
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_sqlquery = run_cell_method

    command = "-s"
    name = "sessions_name"
    context = "-c"
    context_name = "sql"
    quiet = "-q"
    meth = "-m"
    method_name = "sample"
    line = " ".join([command, name, context, context_name, quiet, meth, method_name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(SQLQuery(cell, samplemethod=method_name), name)
    assert result is None


@with_setup(_setup, _teardown)
def test_logs_subcommand():
    get_logs_method = MagicMock()
    result_value = ""
    get_logs_method.return_value = result_value
    spark_controller.get_logs = get_logs_method

    command = "logs -s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    get_logs_method.assert_called_once_with(name)
    assert result is None
    ipython_display.write.assert_called_once_with(result_value)


@with_setup(_setup, _teardown)
def test_logs_exception():
    get_logs_method = MagicMock(side_effect=LivyUnexpectedStatusException('How did this happen?'))
    result_value = ""
    get_logs_method.return_value = result_value
    spark_controller.get_logs = get_logs_method

    command = "logs -s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    get_logs_method.assert_called_once_with(name)
    assert result is None
    ipython_display.send_error.assert_called_once_with(EXPECTED_ERROR_MSG
                                                       .format(get_logs_method.side_effect))
