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
    kernel.use_altair = False
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

    u, p, r = kernel.get_configuration()

    assert u == usr
    assert p == pwd
    assert r == url


@with_setup(_setup, _teardown)
def test_get_config_not_set():
    kernel.send_response = sr_m = MagicMock()
    kernel.do_shutdown = ds_m = MagicMock()

    try:
        kernel.get_configuration()

        # Above should have thrown because env var not set
        assert False
    except KeyError:
        sr_m.assert_called_once_with(None, 'stream', {'text': "FATAL ERROR: Please set configuration for 'USER',"
                                                              " 'PASS', 'URL to initialize Kernel.", 'name': 'stdout'})
        ds_m.assert_called_once_with(False)


@with_setup(_setup, _teardown)
def test_initialize_magics():
    # Set up
    usr = "u"
    pwd = "p"
    url = "url"
    kernel.execute_cell_for_user = execute_cell_mock = MagicMock()
    conn_str = get_connection_string(url, usr, pwd)

    # Call method
    assert not kernel.already_ran_once
    kernel.initialize_magics(usr, pwd, url)

    # Assertions
    assert kernel.already_ran_once
    expected = [call("%spark add TestKernel python {} skip".format(conn_str), True, False),
                call("%load_ext remotespark", True, False)]
    assert len(execute_cell_mock.mock_calls) == 2
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

    kernel.get_configuration = config_mock
    kernel.execute_cell_for_user = execute_cell_mock = MagicMock()

    code = "code"

    # Call method
    assert not kernel.already_ran_once
    kernel.do_execute(code, False)

    # Assertions
    assert kernel.already_ran_once
    assert len(execute_cell_mock.mock_calls) == 3
    assert call("%spark add TestKernel python {} skip".format(conn_str), True, False) in execute_cell_mock.mock_calls
    assert call("%load_ext remotespark", True, False) in execute_cell_mock.mock_calls
    assert call("%%spark\n{}".format(code), False, True, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_call_spark():
    # Set up
    code = "some spark code"
    kernel.already_ran_once = True
    kernel.execute_cell_for_user = execute_cell_mock = MagicMock()

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
        kernel.execute_cell_for_user = execute_cell_mock = MagicMock()

        # Call method
        kernel.do_execute(code, False)

        # Assertions
        assert kernel.already_ran_once
        execute_cell_mock.assert_called_once_with("%%spark -s True\n{}".format(plain_code), False, True, None, False)

    _check("%sql ")
    _check("%sql\n")
    _check("%%sql ")
    _check("%%sql\n")


@with_setup(_setup, _teardown)
def test_shutdown_cleans_up():
    # No restart
    kernel.execute_cell_for_user = ecfu_m = MagicMock()
    kernel.do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel.already_ran_once = True

    kernel.do_shutdown(False)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel.execute_cell_for_user = ecfu_m = MagicMock()
    kernel.do_shutdown_ipykernel = dsi_m = MagicMock()
    kernel.already_ran_once = True

    kernel.do_shutdown(True)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
    dsi_m.assert_called_once_with(True)
