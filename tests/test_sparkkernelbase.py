from mock import MagicMock, call
from nose.tools import with_setup
from remotespark.kernels.wrapperkernel.codetransformers import *
from remotespark.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase

import remotespark.utils.configuration as conf
from remotespark.kernels.wrapperkernel.usercommandparser import UserCommandParser
from remotespark.utils.utils import get_connection_string

kernel = None
user_ev = "username"
pass_ev = "password"
url_ev = "url"
execute_cell_mock = None
do_shutdown_mock = None
conn_str = None
ipython_display = None
parser = None


class TestSparkKernel(SparkKernelBase):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestSparkKernel, self).__init__(None, None, None, None, None, None, None, "TestKernel", **kwargs)


def _setup():
    global kernel, user_ev, pass_ev, url_ev, execute_cell_mock, do_shutdown_mock, conn_str, ipython_display, parser

    usr = "u"
    pwd = "p"
    url = "url"
    conn_str = get_connection_string(url, usr, pwd)

    kernel = TestSparkKernel()
    kernel.use_auto_viz = False
    kernel.kernel_conf_name = "python"
    kernel.username_conf_name = user_ev
    kernel.password_conf_name = pass_ev
    kernel.url_conf_name = url_ev
    kernel.session_language = "python"
    kernel.connection_string = conn_str

    kernel._execute_cell_for_user = execute_cell_mock = MagicMock(return_value={'test':'ing', 'a':'b', 'status':'ok'})
    kernel._do_shutdown_ipykernel = do_shutdown_mock = MagicMock()
    kernel._ipython_display = ipython_display = MagicMock()

    parser = MagicMock()
    parser.parse_user_command.return_value = (UserCommandParser.run_command, False, None, "my code")
    kernel.user_command_parser = parser


def _teardown():
    conf.load()


@with_setup(_setup, _teardown)
def test_get_config():
    usr = "u"
    pwd = "p"
    url = "url"

    config = {conf.kernel_python_credentials.__name__: {user_ev: usr, pass_ev: pwd, url_ev: url}}
    conf.override_all(config)

    u, p, r = kernel._get_configuration()

    assert u == usr
    assert p == pwd
    assert r == url

    conf.load()


@with_setup(_setup, _teardown)
def test_get_config_not_set_empty_strings():
    conf.override_all({conf.kernel_python_credentials.__name__: {user_ev: '', pass_ev: '', url_ev: ''}})
    kernel._get_configuration()
    assert kernel._fatal_error
    conf.load()


@with_setup(_setup, _teardown)
def test_initialize_magics():
    kernel._load_magics_extension()

    assert call("%load_ext remotespark", True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup(), _teardown())
def test_start_session():
    assert not kernel._session_started

    kernel._start_session()

    assert kernel._session_started
    assert call("%spark add TestKernel python {}".format(conn_str), True, False, None, False) \
        in execute_cell_mock.mock_calls
    assert call("%spark delete TestKernel\n%spark add TestKernel python {} skip"
                .format(conn_str), True, False, None, False) not in execute_cell_mock.mock_calls

    # Mark it not started out of bound. This is done by some transformers like cleanup and delete.
    kernel._session_started = False

    kernel._start_session()

    assert kernel._session_started
    assert call("%spark delete TestKernel\n%spark add TestKernel python {}"
                .format(conn_str), True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup(), _teardown())
def test_delete_session():
    kernel._session_started = True

    kernel._delete_session()

    assert not kernel._session_started
    assert call("%spark cleanup", True, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_returns_right_transformer():
    assert type(kernel._get_code_transformer(UserCommandParser.run_command)) is SparkTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.sql_command)) is SqlTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.hive_command)) is HiveTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.config_command)) is ConfigTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.info_command)) is InfoTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.delete_command)) is DeleteSessionTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.clean_up_command)) is CleanUpTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.logs_command)) is LogsTransformer
    assert type(kernel._get_code_transformer("whatever")) is NotSupportedTransformer
    assert type(kernel._get_code_transformer(UserCommandParser.local_command)) is PythonTransformer


@with_setup(_setup, _teardown)
def test_parse_user_command_parser_exception():
    code_to_run = "my code"

    user_command_parser = MagicMock()
    user_command_parser.parse_user_command = MagicMock(side_effect=SyntaxError)
    kernel.user_command_parser = user_command_parser
    kernel._session_started = True
    kernel._show_user_error = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    kernel._show_user_error.assert_called_with("None")
    assert call("None", False, True, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_code():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.do_nothing_action
    end_action = Constants.do_nothing_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    kernel._session_started = True

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    assert not kernel._start_session.called
    assert not kernel._delete_session.called
    assert kernel._session_started


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_error():
    code_to_run = ""
    error_to_show = "error!"
    begin_action = Constants.do_nothing_action
    end_action = Constants.do_nothing_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    kernel._session_started = True

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    kernel._show_user_error.assert_called_with(error_to_show)
    assert ret is execute_cell_mock.return_value
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    assert not kernel._start_session.called
    assert not kernel._delete_session.called
    assert kernel._session_started


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_deletes_session():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.do_nothing_action
    end_action = Constants.do_nothing_action
    deletes_session = True

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    kernel._session_started = True

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    assert not kernel._start_session.called
    assert not kernel._delete_session.called
    assert not kernel._session_started


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_begin_start():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.start_session_action
    end_action = Constants.do_nothing_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    kernel._start_session.assert_called_with()
    assert not kernel._delete_session.called


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_begin_delete():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.delete_session_action
    end_action = Constants.do_nothing_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    assert not kernel._start_session.called
    kernel._delete_session.assert_called_with()


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_end_start():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.do_nothing_action
    end_action = Constants.start_session_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    kernel._start_session.assert_called_with()
    assert not kernel._delete_session.called


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_end_delete():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.do_nothing_action
    end_action = Constants.delete_session_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    assert not kernel._start_session.called
    kernel._delete_session.assert_called_with()


@with_setup(_setup, _teardown)
def test_instructions_from_transformer_are_executed_begin_start_end_delete():
    code_to_run = "my code"
    error_to_show = None
    begin_action = Constants.start_session_action
    end_action = Constants.delete_session_action
    deletes_session = False

    transformer = MagicMock()
    transformer.get_code_to_execute.return_value = (code_to_run, error_to_show, begin_action, end_action,
                                                    deletes_session)
    kernel._get_code_transformer = MagicMock(return_value=transformer)
    kernel._show_user_error = MagicMock()
    kernel._start_session = MagicMock()
    kernel._delete_session = MagicMock()

    # Execute
    ret = kernel.do_execute(code_to_run, False)

    # Assert
    assert ret is execute_cell_mock.return_value
    assert not kernel._show_user_error.called
    assert call(code_to_run, False, True, None, False) in execute_cell_mock.mock_calls
    kernel._start_session.assert_called_with()
    kernel._delete_session.assert_called_with()


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happened():
    # Set up
    fatal_error = "Error."
    code = "some spark code"
    kernel._fatal_error = fatal_error

    ret = kernel.do_execute(code, False)

    assert ret is execute_cell_mock.return_value
    assert kernel._fatal_error == fatal_error
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_execute_alerts_user_if_an_unexpected_error_happens():
    code = "yo"

    transformer = MagicMock()
    transformer.get_code_to_execute = MagicMock(side_effect=ValueError)
    kernel._get_code_transformer = MagicMock(return_value=transformer)

    ret = kernel.do_execute(code, False)

    assert ret is execute_cell_mock.return_value
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happens_for_execution():
    # Set up
    fatal_error = u"Error."
    message = "{}\nException details:\n\t\"{}\"".format(fatal_error, fatal_error)
    code = "some spark code"
    reply_content = dict()
    reply_content[u"status"] = u"error"
    reply_content[u"evalue"] = fatal_error
    execute_cell_mock.return_value = reply_content

    ret = kernel._execute_cell(code, False, shutdown_if_error=True, log_if_error=fatal_error)

    assert ret is execute_cell_mock.return_value
    assert kernel._fatal_error == message
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_shutdown_cleans_up():
    # No restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel._session_started = True

    kernel.do_shutdown(False)

    assert not kernel._session_started
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel._session_started = True

    kernel.do_shutdown(True)

    assert not kernel._session_started
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(True)


@with_setup(_setup, _teardown)
def test_register_auto_viz():
    kernel._register_auto_viz()

    assert call("from remotespark.datawidgets.utils import display_dataframe\nip = get_ipython()\nip.display_formatter"
                ".ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)",
                True, False, None, False) in execute_cell_mock.mock_calls
