from mock import MagicMock, call
from nose.tools import with_setup, raises

from remotespark.sparkkernelbase import SparkKernelBase
import remotespark.utils.configuration as conf
from remotespark.utils.utils import get_connection_string

kernel = None
user_ev = "username"
pass_ev = "password"
url_ev = "url"
send_error_mock = None
execute_cell_mock = None
do_shutdown_mock = None
conn_str = None


class TestSparkKernel(SparkKernelBase):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestSparkKernel, self).__init__(None, None, None, None, None, None, None, "TestKernel", **kwargs)


def _setup():
    global kernel, user_ev, pass_ev, url_ev, send_error_mock, execute_cell_mock, do_shutdown_mock, conn_str

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

    kernel._ipython_send_error = send_error_mock = MagicMock()
    kernel._execute_cell_for_user = execute_cell_mock = MagicMock()
    kernel._do_shutdown_ipykernel = do_shutdown_mock = MagicMock()


def _teardown():
    conf.load()


@with_setup(_setup, _teardown)
def test_get_config():
    usr = "u"
    pwd = "p"
    url = "url"

    config = {"kernel_python_credentials": {user_ev: usr, pass_ev: pwd, url_ev: url}}
    conf.override_all(config)

    u, p, r = kernel._get_configuration()

    assert u == usr
    assert p == pwd
    assert r == url

    conf.load()


@with_setup(_setup, _teardown)
def test_get_config_not_set():
    conf.override_all({})
    try:
        kernel._get_configuration()

        # Above should have thrown because env var not set
        assert False
    except ValueError:
        assert send_error_mock.call_count == 1
    conf.load()


@with_setup(_setup, _teardown)
def test_initialize_magics():
    kernel._load_magics_extension()

    assert call("%load_ext remotespark", True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup(), _teardown())
def test_start_session():
    assert not kernel.session_started

    kernel._start_session()

    assert kernel.session_started
    assert call("%spark add TestKernel python {} skip".format(conn_str), True, False, None, False) \
        in execute_cell_mock.mock_calls


@with_setup(_setup(), _teardown())
def test_delete_session():
    kernel.session_started = True

    kernel._delete_session()

    assert not kernel.session_started
    assert call("%spark cleanup", True, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_set_config():
    def _check(prepend, session_started=False, key_error_expected=False):
        # Set up
        kernel._show_user_error = MagicMock(side_effect=KeyError)
        properties = """{"extra": 2}"""
        code = prepend + properties
        kernel.session_started = session_started
        execute_cell_mock.reset_mock()

        # Call method
        try:
            kernel.do_execute(code, False)
        except KeyError:
            if not key_error_expected:
                assert False

            # When exception is expected, nothing to check
            return

        assert session_started == kernel.session_started
        assert call("%spark config {}".format(properties), False, True, None, False) \
            in execute_cell_mock.mock_calls

        if session_started and not key_error_expected:
            # This means -f must be present, so check that a restart happened
            assert call("%spark cleanup", True, False) in execute_cell_mock.mock_calls
            assert call("%spark add TestKernel python {} skip".format(conn_str), True, False, None, False) \
                in execute_cell_mock.mock_calls

    _check("%config ")
    _check("%config\n")
    _check("%%config ")
    _check("%%config\n")
    _check("%config -f ")
    _check("%config ", True, True)
    _check("%config -f ", True, False)


@with_setup(_setup, _teardown)
def test_do_execute_initializes_magics_if_not_run():
    code = "code"

    # Call method
    assert not kernel.session_started
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.session_started
    assert call("%spark add TestKernel python {} skip"
                .format(conn_str), True, False, None, False) in execute_cell_mock.mock_calls
    assert call("%%spark\n{}".format(code), False, True, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
@raises(KeyError)
def test_magic_not_supported():
    # Set up
    code = "%alex some spark code"
    kernel.session_started = True
    kernel._show_user_error = MagicMock(side_effect=KeyError)

    # Call method
    kernel.do_execute(code, False)


@with_setup(_setup, _teardown)
def test_info():
    code = "%info"

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert not kernel.session_started
    assert call("%spark info {}".format(conn_str), False, True, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_delete_force():
    code = "%delete -f 9"
    kernel.session_started = True
    user_error = MagicMock()
    kernel._show_user_error = user_error

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert not kernel.session_started
    assert call("%spark delete {} 9".format(conn_str), False, True, None, False) in execute_cell_mock.mock_calls
    assert len(user_error.mock_calls) == 0


@with_setup(_setup, _teardown)
def test_delete_not_force():
    code = "%delete 9"
    kernel.session_started = True
    user_error = MagicMock()
    kernel._show_user_error = user_error

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.session_started
    assert not call("%spark delete {} 9".format(conn_str), False, True, None, False) in execute_cell_mock.mock_calls
    assert len(user_error.mock_calls) == 1


@with_setup(_setup, _teardown)
def test_cleanup_force():
    code = "%cleanup -f"
    kernel.session_started = True
    user_error = MagicMock()
    kernel._show_user_error = user_error

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert not kernel.session_started
    assert call("%spark cleanup {}".format(conn_str), False, True, None, False) in execute_cell_mock.mock_calls
    assert len(user_error.mock_calls) == 0


@with_setup(_setup, _teardown)
def test_cleanup_not_force():
    code = "%cleanup"
    kernel.session_started = True
    user_error = MagicMock()
    kernel._show_user_error = user_error

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.session_started
    assert not call("%spark cleanup {}".format(conn_str), False, True, None, False) in execute_cell_mock.mock_calls
    assert len(user_error.mock_calls) == 1


@with_setup(_setup, _teardown)
def test_call_spark():
    # Set up
    code = "some spark code"
    kernel.session_started = True

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.session_started
    execute_cell_mock.assert_called_once_with("%%spark\n{}".format(code), False, True, None, False)


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happened():
    # Set up
    fatal_error = "Error."
    code = "some spark code"
    kernel._fatal_error = fatal_error

    # Call method
    try:
        kernel.do_execute(code, False)

        # Fail if not thrown
        assert False
    except ValueError:
        # Assertions
        assert kernel._fatal_error == fatal_error
        assert execute_cell_mock.call_count == 0
        assert send_error_mock.call_count == 1


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happens_for_execution():
    # Set up
    fatal_error = u"Error."
    message = "{}\nException details:\n\t\"{}\"".format(fatal_error, fatal_error)
    stream_content = {"name": "stderr", "text": conf.fatal_error_suggestion().format(message)}
    code = "some spark code"
    reply_content = dict()
    reply_content[u"status"] = u"error"
    reply_content[u"evalue"] = fatal_error
    execute_cell_mock.return_value = reply_content

    # Call method
    try:
        kernel._execute_cell(code, False, shutdown_if_error=True, log_if_error=fatal_error)

        # Fail if not thrown
        assert False
    except ValueError:
        # Assertions
        assert kernel._fatal_error == message
        assert execute_cell_mock.call_count == 1
        send_error_mock.assert_called_once_with(stream_content)


@with_setup(_setup, _teardown)
def test_call_spark_sql_new_line():
    def _check(prepend):
        # Set up
        plain_code = "select tables"
        code = prepend + plain_code
        kernel.session_started = True
        execute_cell_mock.reset_mock()

        # Call method
        kernel.do_execute(code, False)

        # Assertions
        assert kernel.session_started
        execute_cell_mock.assert_called_once_with("%%spark -c sql\n{}".format(plain_code), False, True, None, False)

    _check("%sql ")
    _check("%sql\n")
    _check("%%sql ")
    _check("%%sql\n")


@with_setup(_setup, _teardown)
def test_call_spark_hive_new_line():
    def _check(prepend):
        # Set up
        plain_code = "select tables"
        code = prepend + plain_code
        kernel.session_started = True
        execute_cell_mock.reset_mock()

        # Call method
        kernel.do_execute(code, False)

        # Assertions
        assert kernel.session_started
        execute_cell_mock.assert_called_once_with("%%spark -c hive\n{}".format(plain_code), False, True, None, False)

    _check("%hive ")
    _check("%hive\n")
    _check("%%hive ")
    _check("%%hive\n")


@with_setup(_setup, _teardown)
def test_shutdown_cleans_up():
    # No restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel.session_started = True

    kernel.do_shutdown(False)

    assert not kernel.session_started
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel.session_started = True

    kernel.do_shutdown(True)

    assert not kernel.session_started
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(True)


@with_setup(_setup, _teardown)
def test_register_auto_viz():
    kernel._register_auto_viz()

    assert call("from remotespark.datawidgets.utils import display_dataframe\nip = get_ipython()\nip.display_formatter"
                ".ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)",
                True, False, None, False) in execute_cell_mock.mock_calls
