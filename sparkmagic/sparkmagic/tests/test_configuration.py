import pytest
from mock import MagicMock

import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.constants import AUTH_BASIC, NO_AUTH


def setup_function():
    conf.override_all({})


def test_configuration_override_base64_password():
    kpc = {
        "username": "U",
        "password": "P",
        "base64_password": "cGFzc3dvcmQ=",
        "url": "L",
        "auth": AUTH_BASIC,
    }
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    conf.override_all(overrides)
    conf.override(conf.livy_session_startup_timeout_seconds.__name__, 1)
    assert conf.d == {
        conf.kernel_python_credentials.__name__: kpc,
        conf.livy_session_startup_timeout_seconds.__name__: 1,
    }
    assert conf.livy_session_startup_timeout_seconds() == 1
    assert conf.base64_kernel_python_credentials() == {
        "username": "U",
        "password": "password",
        "url": "L",
        "auth": AUTH_BASIC,
    }


def test_configuration_auth_missing_basic_auth():
    kpc = {"username": "U", "password": "P", "url": "L"}
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    conf.override_all(overrides)
    assert conf.base64_kernel_python_credentials() == {
        "username": "U",
        "password": "P",
        "url": "L",
        "auth": AUTH_BASIC,
    }


def test_configuration_auth_missing_no_auth():
    kpc = {"username": "", "password": "", "url": "L"}
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    conf.override_all(overrides)
    assert conf.base64_kernel_python_credentials() == {
        "username": "",
        "password": "",
        "url": "L",
        "auth": NO_AUTH,
    }


def test_configuration_override_fallback_to_password():
    kpc = {"username": "U", "password": "P", "url": "L", "auth": NO_AUTH}
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    conf.override_all(overrides)
    conf.override(conf.livy_session_startup_timeout_seconds.__name__, 1)
    assert conf.d == {
        conf.kernel_python_credentials.__name__: kpc,
        conf.livy_session_startup_timeout_seconds.__name__: 1,
    }
    assert conf.livy_session_startup_timeout_seconds() == 1
    assert conf.base64_kernel_python_credentials() == kpc


def test_configuration_override_work_with_empty_password():
    kpc = {
        "username": "U",
        "base64_password": "",
        "password": "",
        "url": "",
        "auth": AUTH_BASIC,
    }
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    conf.override_all(overrides)
    conf.override(conf.livy_session_startup_timeout_seconds.__name__, 1)
    assert conf.d == {
        conf.kernel_python_credentials.__name__: kpc,
        conf.livy_session_startup_timeout_seconds.__name__: 1,
    }
    assert conf.livy_session_startup_timeout_seconds() == 1
    assert conf.base64_kernel_python_credentials() == {
        "username": "U",
        "password": "",
        "url": "",
        "auth": AUTH_BASIC,
    }


def test_configuration_raise_error_for_bad_base64_password():
    with pytest.raises(BadUserConfigurationException):
        kpc = {"username": "U", "base64_password": "P", "url": "L"}
        overrides = {conf.kernel_python_credentials.__name__: kpc}
        conf.override_all(overrides)
        conf.override(conf.livy_session_startup_timeout_seconds.__name__, 1)
        conf.base64_kernel_python_credentials()


def test_share_config_between_pyspark_and_pyspark3():
    kpc = {
        "username": "U",
        "password": "P",
        "base64_password": "cGFzc3dvcmQ=",
        "url": "L",
        "auth": AUTH_BASIC,
    }
    overrides = {conf.kernel_python_credentials.__name__: kpc}
    assert (
        conf.base64_kernel_python3_credentials()
        == conf.base64_kernel_python_credentials()
    )
