from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, raises, with_setup
import json

import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.constants import BASIC_AUTH

def _setup():
    conf.override_all({})
    

@with_setup(_setup)
def test_configuration_override_base64_password():
    kpc = { 'username': 'U', 'password': 'P', 'base64_password': 'cGFzc3dvcmQ=', u'authentication': BASIC_AUTH, 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf.d, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(), { 'username': 'U', 'password': 'password', u'authentication': BASIC_AUTH, 'url': 'L' })


@with_setup(_setup)
def test_configuration_override_fallback_to_password():
    kpc = { 'username': 'U', 'password': 'P', u'authentication': BASIC_AUTH, 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf.d, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(), kpc)


@with_setup(_setup)
def test_configuration_override_work_with_empty_password():
    kpc = { 'username': 'U', 'base64_password': '', 'password': '', u'authentication': '', 'url': '' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf.d, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.base64_kernel_python_credentials(),  { 'username': 'U', 'password': '', u'authentication': '', 'url': '' })


@raises(BadUserConfigurationException)
@with_setup(_setup)
def test_configuration_raise_error_for_bad_base64_password():
    kpc = { 'username': 'U', 'base64_password': 'P', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    conf.base64_kernel_python_credentials()


@with_setup(_setup)
def test_share_config_between_pyspark_and_pyspark3():
    kpc = { 'username': 'U', 'password': 'P', 'base64_password': 'cGFzc3dvcmQ=', u'authentication': BASIC_AUTH, 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    assert_equals(conf.base64_kernel_python3_credentials(), conf.base64_kernel_python_credentials())
