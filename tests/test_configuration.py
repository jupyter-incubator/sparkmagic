import remotespark.utils.configuration as conf
from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, raises, with_setup
from remotespark.livyclientlib.exceptions import BadUserConfigurationException
import json

def _setup():
    conf._overrides = None


@with_setup(_setup)
def test_configuration_initialize():
    s = "hello"
    config = { conf.fatal_error_suggestion.__name__: s }
    fsrw = MagicMock()
    fsrw.path = ""
    read_lines = MagicMock(return_value=[json.dumps(config)])
    fsrw.read_lines = read_lines
    fsrw_class = MagicMock(return_value=fsrw)
    conf.initialize(fsrw_class)
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.fatal_error_suggestion(), s)



@with_setup(_setup)
def test_configuration_initialize_lazy():
    """Tests that the initialize function has no behavior if the override dict is already initialized"""
    conf.override_all({})
    fsrw_class = MagicMock(side_effect=ValueError)
    conf.initialize(fsrw_class)


@with_setup(_setup)
def test_configuration_load():
    i = 1000
    config = { conf.statement_sleep_seconds.__name__: i }
    fsrw = MagicMock()
    fsrw.path = ""
    read_lines = MagicMock(return_value=[json.dumps(config)])
    fsrw.read_lines = read_lines
    fsrw_class = MagicMock(return_value=fsrw)
    conf.load(fsrw_class)
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.statement_sleep_seconds(), i)


@with_setup(_setup)
def test_configuration_load_not_lazy():
    a = "whoops"
    config = { conf.events_handler_class.__name__: a }
    fsrw = MagicMock()
    fsrw.path = ""
    read_lines = MagicMock(return_value=[json.dumps(config)])
    fsrw.read_lines = read_lines
    fsrw_class = MagicMock(return_value=fsrw)
    conf.override_all({conf.events_handler_class.__name__: "bar"})
    conf.load(fsrw_class)
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.events_handler_class(), a)


@with_setup(_setup)
def test_configuration_override_base64_password():
    kpc = { 'username': 'U', 'password': 'P', 'base64_password': 'cGFzc3dvcmQ=', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf._overrides, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.kernel_python_credentials(), { 'username': 'U', 'password': 'password', 'url': 'L' })


@with_setup(_setup)
def test_configuration_override_fallback_to_password():
    kpc = { 'username': 'U', 'password': 'P', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    assert_equals(conf._overrides, { conf.kernel_python_credentials.__name__: kpc,
                                     conf.status_sleep_seconds.__name__: 1 })
    assert_equals(conf.status_sleep_seconds(), 1)
    assert_equals(conf.kernel_python_credentials(), kpc)


@raises(BadUserConfigurationException)
@with_setup(_setup)
def test_configuration_raise_error_for_bad_base64_password():
    kpc = { 'username': 'U', 'base64_password': 'P', 'url': 'L' }
    overrides = { conf.kernel_python_credentials.__name__: kpc }
    conf.override_all(overrides)
    conf.override(conf.status_sleep_seconds.__name__, 1)
    conf.kernel_python_credentials()


@with_setup(_setup)
def test_configuration_override_all():
    z = 1500
    config = { conf.status_sleep_seconds.__name__: z }
    conf.override_all(config)
    assert_equals(conf._overrides, config)
    assert_equals(conf.status_sleep_seconds(), z)


@with_setup(_setup)
def test_configuration_decorator():
    def test_f():
        return 0
    conf.override_all({test_f.__name__: -1})
    test_f_decorated = conf._override(test_f)
    assert_not_equals(test_f_decorated(), test_f())
    assert_equals(test_f_decorated(), -1)