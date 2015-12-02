from nose.tools import with_setup
from mock import MagicMock, call

from remotespark.sparkkernelbase import SparkKernelBase
from remotespark.livyclientlib.utils import get_connection_string
from remotespark.livyclientlib.configuration import _t_config_hook


kernel = None
user_ev = "USER"
pass_ev = "PASS"
url_ev = "URL"


class TestSparkKernel(SparkKernelBase):
    client_name = "TestKernel"


def _setup():
    global kernel, user_ev, pass_ev, url_ev

    kernel = TestSparkKernel()
    kernel.use_auto_viz = False
    kernel.username_conf_name = user_ev
    kernel.password_conf_name = pass_ev
    kernel.url_conf_name = url_ev
    kernel.session_language = "python"


def _teardown():
    _t_config_hook({})


@with_setup(_setup, _teardown)
def test_get_config():
    usr = "u"
    pwd = "p"
    url = "url"

    config = {user_ev: usr, pass_ev: pwd, url_ev: url}
    _t_config_hook(config)

    u, p, r = kernel._get_configuration()

    assert u == usr
    assert p == pwd
    assert r == url


@with_setup(_setup, _teardown)
def test_get_config_not_set():
    kernel.do_shutdown = ds_m = MagicMock()
    kernel._ipython_send_error = se_m = MagicMock()

    try:
        kernel._get_configuration()

        # Above should have thrown because env var not set
        assert False
    except ValueError:
        # ds_m.assert_called_once_with(False)
        pass


@with_setup(_setup, _teardown)
def test_initialize_magics():
    # Set up
    usr = "u"
    pwd = "p"
    url = "url"
    kernel._execute_cell_for_user = execute_cell_mock = MagicMock()
    conn_str = get_connection_string(url, usr, pwd)

    # Call method
    assert not kernel.already_ran_once
    kernel._initialize_magics(usr, pwd, url)

    # Assertions
    assert kernel.already_ran_once
    expected = [call("%spark add TestKernel python {} skip".format(conn_str), True, False, None, False),
                call("%load_ext remotespark", True, False, None, False)]
    for kall in expected:
        assert kall in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_do_execute_initializes_magics_if_not_run():
    # Set up
    usr = "u"
    pwd = "p"
    url = "url"
    conn_str = get_connection_string(url, usr, pwd)
    config_mock = MagicMock()
    config_mock.return_value = (usr, pwd, url)

    kernel._get_configuration = config_mock
    kernel._execute_cell_for_user = execute_cell_mock = MagicMock()

    code = "code"

    # Call method
    assert not kernel.already_ran_once
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.already_ran_once
    assert call("%spark add TestKernel python {} skip".format(conn_str), True, False, None, False) in execute_cell_mock.mock_calls
    assert call("%load_ext remotespark", True, False, None, False) in execute_cell_mock.mock_calls
    assert call("%%spark\n{}".format(code), False, True, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_call_spark():
    # Set up
    code = "some spark code"
    kernel.already_ran_once = True
    kernel._execute_cell_for_user = execute_cell_mock = MagicMock()

    # Call method
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.already_ran_once
    execute_cell_mock.assert_called_once_with("%%spark\n{}".format(code), False, True, None, False)


@with_setup(_setup, _teardown)
def test_call_spark_sql_new_line():
    def _check(prepend):
        # Set up
        plain_code = "select tables"
        code = prepend + plain_code
        kernel.already_ran_once = True
        kernel._execute_cell_for_user = execute_cell_mock = MagicMock()

        # Call method
        kernel.do_execute(code, False)

        # Assertions
        assert kernel.already_ran_once
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
        kernel.already_ran_once = True
        kernel._execute_cell_for_user = execute_cell_mock = MagicMock()

        # Call method
        kernel.do_execute(code, False)

        # Assertions
        assert kernel.already_ran_once
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
    kernel.already_ran_once = True

    kernel.do_shutdown(False)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel.already_ran_once = True

    kernel.do_shutdown(True)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(True)
