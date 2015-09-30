from nose.tools import raises, with_setup, assert_equals
from remotespark.RemoteSparkMagics import RemoteSparkMagics
from mock import MagicMock

ip = get_ipython()
magic = None

def _setup():
	global magic
	magic = RemoteSparkMagics(shell=ip)
	ip.register_magics(magic)

def _teardown():
	pass

@with_setup(_setup, _teardown)
def test_info_command():
	mock_method = MagicMock()
	magic._print_info = mock_method
	command = "info"

	ip.run_line_magic("spark", command)

	mock_method.assert_called_with()

@with_setup(_setup, _teardown)
def test_add_endpoint_command():
	mock_method = MagicMock()
	magic.add_endpoint = mock_method
	command = "add"
	name = "name"
	language = "python"
	connection_string = "url=http://location:port;username=name;password=word"
	line = " ".join([command, name, language, connection_string])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_with(name, language, connection_string)


@with_setup(_setup, _teardown)
def test_delete_endpoint_command():
	mock_method = MagicMock()
	magic.delete_endpoint = mock_method
	command = "delete"
	name = "name"
	line = " ".join([command, name])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_with(name)

@with_setup(_setup, _teardown)
def test_mode_command():
	mock_method = MagicMock()
	magic.log_mode = mock_method
	command = "mode"
	mode = "debug"

	line = " ".join([command, mode])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_with(mode)

@with_setup(_setup, _teardown)
def test_cleanup_command():
	mock_method = MagicMock()
	magic.cleanup = mock_method
	command = "cleanup"

	ip.run_line_magic("spark", command)

	mock_method.assert_called_with()

@raises(ValueError)
@with_setup(_setup, _teardown)
def test_bad_command():
	command = "bad_command"

	ip.run_line_magic("spark", command)

@with_setup(_setup, _teardown)
def test_run_cell():
	mock_method = MagicMock()
	magic.run_cell = mock_method
	command = "-c"
	name = "endpoint_name"
	line = " ".join([command, name])
	cell = "cell code"

	ip.run_cell_magic("spark", line, cell)

	mock_method.assert_called_with(name, False, cell)