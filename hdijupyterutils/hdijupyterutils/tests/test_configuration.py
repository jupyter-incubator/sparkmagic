from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, raises, with_setup
import json

from hdijupyterutils.configuration import Configuration


conf = None
fsrw_class = None


class MyConfiguration(Configuration):
    def __init__(self):
        super(MyConfiguration, self).__init__("", "")
        
    @Configuration._override
    def my_conf(self):
        return "hi"


def get_fsrw(config):
    fsrw = MagicMock()
    fsrw.path = ""
    read_lines = MagicMock(return_value=[json.dumps(config)])
    fsrw.read_lines = read_lines
    fsrw_class = MagicMock(return_value=fsrw)
    return fsrw_class


def _setup():
    global conf, fsrw_class
    
    fsrw_class = get_fsrw({})
    
    conf = MyConfiguration()
    conf._overrides = None


@with_setup(_setup)
def test_configuration_initialize():
    s = "hello"
    config = { conf.my_conf.__name__: s }
    conf.fsrw_class = get_fsrw(config)
    conf.initialize()
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.my_conf(), s)



@with_setup(_setup)
def test_configuration_initialize_lazy():
    """Tests that the initialize function has no behavior if the override dict is already initialized"""
    config = {}
    conf.override_all(config)
    conf.fsrw_class = get_fsrw(config)
    conf.initialize()


@with_setup(_setup)
def test_configuration_load():
    i = 1000
    config = { conf.my_conf.__name__: i }
    conf.fsrw_class = get_fsrw(config)
    conf.load()
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.my_conf(), i)


@with_setup(_setup)
def test_configuration_load_not_lazy():
    a = "whoops"
    config = { conf.my_conf.__name__: a }
    conf.fsrw_class = get_fsrw(config)
    conf.override_all({conf.my_conf.__name__: "bar"})
    conf.load()
    assert conf._overrides is not None
    assert_equals(conf._overrides, config)
    assert_equals(conf.my_conf(), a)
    

@with_setup(_setup)
def test_configuration_override_all():
    z = 1500
    config = { conf.my_conf.__name__: z }
    conf.override_all(config)
    assert_equals(conf._overrides, config)
    assert_equals(conf.my_conf(), z)
