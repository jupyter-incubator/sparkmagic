from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, raises, with_setup
import json

from sparkmagic.utils.configuration import SparkMagicConfiguration
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


def _setup():
    global conf
    
    conf = SparkMagicConfiguration()
    conf._overrides = None
    

@with_setup(_setup)
def test_configuration_override_base64_password():
    kpc = { 'username': 'U', 'password': 'P', 'base64_password': 'cGFzc3dvcmQ=', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf._overrides, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(), { 'username': 'U', 'password': 'password', 'url': 'L' })


@with_setup(_setup)
def test_configuration_override_fallback_to_password():
    kpc = { 'username': 'U', 'password': 'P', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf._overrides, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(), kpc)


@with_setup(_setup)
def test_configuration_override_work_with_empty_password():
    kpc = { 'username': 'U', 'base64_password': '', 'password': '', 'url': '' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf._overrides, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(),  { 'username': 'U', 'password': '', 'url': '' })


@raises(BadUserConfigurationException)
@with_setup(_setup)
def test_configuration_raise_error_for_bad_base64_password():
    kpc = { 'username': 'U', 'base64_password': 'P', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    conf.base64_kernel_python_credentials()
