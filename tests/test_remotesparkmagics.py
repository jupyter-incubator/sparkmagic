from nose.tools import raises, with_setup, assert_equals
from mock import MagicMock

from remotespark.livyclientlib.log import Log
from remotespark.RemoteSparkMagics import RemoteSparkMagics



ip = get_ipython()
magic = None
client_manager = None
client_factory = None

def _setup():
	global magic, client_manager, client_factory
	magic = RemoteSparkMagics(shell=ip)
	ip.register_magics(magic)

	client_manager = MagicMock()
	client_factory = MagicMock()
	magic.client_manager = client_manager
	magic.client_factory = client_factory

def _teardown():
	pass

@with_setup(_setup, _teardown)
def test_info_command_parses():
	mock_method = MagicMock()
	magic._print_info = mock_method
	command = "info"

	ip.run_line_magic("spark", command)

	mock_method.assert_called_once_with()

@with_setup(_setup, _teardown)
def test_add_endpoint_command_parses():
	mock_method = MagicMock()
	magic.add_endpoint = mock_method
	command = "add"
	name = "name"
	language = "python"
	connection_string = "url=http://location:port;username=name;password=word"
	line = " ".join([command, name, language, connection_string])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_once_with(name, language, connection_string)

@with_setup(_setup, _teardown)
def test_add_endpoint():
	name = "name"
	language = "python"
	connection_string = "url=http://location:port;username=name;password=word"
	client = "client"
	session = MagicMock()
	client_factory.create_session = MagicMock(return_value=session)
	client_factory.build_client = MagicMock(return_value=client)

	magic.add_endpoint(name, language, connection_string)

	client_factory.create_session.assert_called_once_with(language, connection_string, "-1", False)
	client_factory.build_client.assert_called_once_with(language, session)
	client_manager.add_client.assert_called_once_with(name, client)
	session.start.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_endpoint_command_parses():
	mock_method = MagicMock()
	magic.delete_endpoint = mock_method
	command = "delete"
	name = "name"
	line = " ".join([command, name])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_once_with(name)

@with_setup(_setup, _teardown)
def test_delete_endpoint():
	name = "name"

	magic.delete_endpoint(name)

	client_manager.delete_client.assert_called_once_with(name)

@with_setup(_setup, _teardown)
def test_mode_command_parses():
	mock_method = MagicMock()
	magic.log_mode = mock_method
	command = "mode"
	mode = "debug"

	line = " ".join([command, mode])

	ip.run_line_magic("spark", line)

	mock_method.assert_called_once_with(mode)

@with_setup(_setup, _teardown)
def test_delete_endpoint():
	mode = "debug"

	magic.log_mode(mode)

	assert_equals(mode, Log.mode)

@with_setup(_setup, _teardown)
def test_cleanup_command_parses():
	mock_method = MagicMock()
	magic.cleanup = mock_method
	command = "cleanup"

	ip.run_line_magic("spark", command)

	mock_method.assert_called_once_with()

@with_setup(_setup, _teardown)
def test_cleanup():
	magic.cleanup()
	client_manager.clean_up_all.assert_called_once_with()

@raises(ValueError)
@with_setup(_setup, _teardown)
def test_bad_command_throws_exception():
	command = "bad_command"

	ip.run_line_magic("spark", command)

@with_setup(_setup, _teardown)
def test_run_cell_command_parses():
	mock_method = MagicMock()
	magic.run_cell = mock_method
	command = "-c"
	name = "endpoint_name"
	line = " ".join([command, name])
	cell = "cell code"

	ip.run_cell_magic("spark", line, cell)

	mock_method.assert_called_once_with(name, False, cell)

@with_setup(_setup, _teardown)
def test_run_cell():
	default_client = MagicMock()
	chosen_client = MagicMock()
	client_manager.get_any_client = MagicMock(return_value=default_client)
	client_manager.get_client = MagicMock(return_value=chosen_client)
	name = "endpoint_name"
	sql = False
	cell = "cell code"

	magic.run_cell(name, sql, cell)
	chosen_client.execute.assert_called_with(cell)

	magic.run_cell(None, sql, cell)
	default_client.execute.assert_called_with(cell)

	sql = True

	magic.run_cell(name, sql, cell)
	chosen_client.execute_sql.assert_called_with(cell)

	magic.run_cell(None, sql, cell)
	default_client.execute_sql.assert_called_with(cell)
