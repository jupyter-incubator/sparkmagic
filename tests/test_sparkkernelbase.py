from nose.tools import with_setup
from mock import MagicMock, call
import os

from remotespark.sparkkernelbase import SparkKernelBase
from remotespark.livyclientlib.connectionstringutil import get_connection_string


kernel = None
user_ev = "USER"
pass_ev = "PASS"
url_ev = "URL"


def _setup():
    global kernel, user_ev, pass_ev, url_ev

    kernel = SparkKernelBase()
    kernel.use_altair = False
    kernel.username_env_var = user_ev
    kernel.password_env_var = pass_ev
    kernel.url_env_var = url_ev
    kernel.session_language = "python"
    kernel.client_name = "test"


def _teardown():
    global user_ev, pass_ev, url_ev

    os.environ.pop(user_ev, None)
    os.environ.pop(pass_ev, None)
    os.environ.pop(url_ev, None)


@with_setup(_setup, _teardown)
def test_get_config():
    usr = "u"
    pwd = "p"
    url = "url"

    os.environ[user_ev] = usr
    os.environ[pass_ev] = pwd
    os.environ[url_ev] = url

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
        sr_m.assert_called_once_with(None, 'stream', {'text': "FATAL ERROR: Please set environment variables 'USER',"
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
    expected = [call("%spark add test python {}".format(conn_str), True, False),
                call("%load_ext remotespark\nimport requests\nrequests.packages.urllib3.disable_warnings()",
                     True, False)]
    execute_cell_mock.mock_calls == expected


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
def test_shutdown_cleansup():
    # No restart
    kernel.execute_cell_for_user = ecfu_m = MagicMock()
    kernel.already_ran_once = True

    kernel.do_shutdown(False)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)

    # On restart
    kernel.execute_cell_for_user = ecfu_m = MagicMock()
    kernel.already_ran_once = True

    kernel.do_shutdown(True)

    assert not kernel.already_ran_once
    ecfu_m.assert_called_once_with("%spark cleanup", True, False)
