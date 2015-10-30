from nose.tools import raises, with_setup
from mock import MagicMock

from remotespark.RemoteSparkMagics import RemoteSparkMagics
from remotespark.livyclientlib.rawviewer import RawViewer
from remotespark.livyclientlib.altairviewer import AltairViewer
from remotespark.livyclientlib.constants import Constants


magic = None
spark_controller = None
viewer = None


def _setup():
    global magic, spark_controller, viewer
    magic = RemoteSparkMagics(shell=None, test=True)

    spark_controller = MagicMock()
    viewer = MagicMock
    magic.spark_controller = spark_controller
    magic.viewer = viewer


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
def test_add_endpoint_command_parses():
    # Do not skip
    add_endpoint_mock = MagicMock()
    spark_controller.add_endpoint = add_endpoint_mock
    command = "add"
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string])

    magic.spark(line)

    add_endpoint_mock.assert_called_once_with(name, language, connection_string, False)

    # Skip
    add_endpoint_mock = MagicMock()
    spark_controller.add_endpoint = add_endpoint_mock
    command = "add"
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    line = " ".join([command, name, language, connection_string, "skip"])

    magic.spark(line)

    add_endpoint_mock.assert_called_once_with(name, language, connection_string, True)


@with_setup(_setup, _teardown)
def test_delete_endpoint_command_parses():
    mock_method = MagicMock()
    spark_controller.delete_endpoint = mock_method
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
    run_cell_method.return_value = 1
    spark_controller.run_cell = run_cell_method
    visualize_method = MagicMock()
    viewer.visualize = visualize_method

    command = "-e"
    name = "endpoint_name"
    line = " ".join([command, name])
    cell = "cell code"

    magic.spark(line, cell)

    run_cell_method.assert_called_once_with(name, Constants.context_name_spark, cell)
    visualize_method.assert_called_once_with(1, "area")


@with_setup(_setup, _teardown)
def test_viewer_command():
    command = "viewer auto"
    magic.viewer = None

    magic.spark(command)

    assert type(magic.viewer) == AltairViewer

    command = "viewer df"
    magic.viewer = None

    magic.spark(command)

    assert type(magic.viewer) == RawViewer
