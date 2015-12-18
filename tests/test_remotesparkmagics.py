from nose.tools import raises, with_setup
from mock import MagicMock

from remotespark.RemoteSparkMagics import RemoteSparkMagics
from remotespark.livyclientlib.constants import Constants


magic = None
spark_controller = None

def _setup():
    global magic, spark_controller, viewer
    magic = RemoteSparkMagics(shell=None, test=True)

    spark_controller = MagicMock()
    magic.spark_controller = spark_controller


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_info_command_parses():
    print_info_mock = MagicMock()
    magic._print_info = print_info_mock
    command = "info"

    magic.spark(command)

    print_info_mock.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_add_sessions_command_parses():
    # Do not skip
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with(name, language, connection_string, False)

    # Skip
    add_sessions_mock = MagicMock()
    spark_controller.add_session = add_sessions_mock
    command = "add"
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string, "skip"])

    magic.spark(line)

    add_sessions_mock.assert_called_once_with(name, language, connection_string, True)


@with_setup(_setup, _teardown)
def test_delete_sessions_command_parses():
    mock_method = MagicMock()
    spark_controller.delete_session = mock_method
    command = "delete"
    name = "name"
    line = " ".join([command, name])

    magic.spark(line)

    mock_method.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_cleanup_command_parses():
    mock_method = MagicMock()
    spark_controller.cleanup = mock_method
    line = "cleanup"

    magic.spark(line)

    mock_method.assert_called_once_with()


@raises(ValueError)
@with_setup(_setup, _teardown)
def test_bad_command_throws_exception():
    line = "bad_command"

    magic.spark(line)


@with_setup(_setup, _teardown)
def test_run_cell_command_parses():
    run_cell_method = MagicMock()
    run_cell_method.return_value = (True, "")
    spark_controller.run_cell = run_cell_method

    command = "-s"
    name = "sessions_name"
    line = " ".join([command, name])
    cell = "cell code"

    result = magic.spark(line, cell)

    run_cell_method.assert_called_once_with(cell, name)

