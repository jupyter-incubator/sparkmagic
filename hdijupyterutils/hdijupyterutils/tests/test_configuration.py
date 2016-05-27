from mock import MagicMock
from nose.tools import assert_equals, assert_not_equals, raises, with_setup
import json

from hdijupyterutils.configuration import override, override_all, with_override


# This is a sample implementation of how a module would use the config methods.
# We'll use these three functions to test it works.
d = {}
path = "config.json"
original_value = 0

    
def module_override(config, value):
    global d, path
    override(d, path, config, value)


def module_override_all(obj):
    global d
    override_all(d, obj)


# Configs
@with_override(d, path)
def my_config():
    global original_value
    return original_value
    
    
@with_override(d, path)
def my_config_2():
    global original_value
    return original_value    
    
    
# Test helper functions
def _setup():
    module_override_all({})
    
    
def _teardown():
    module_override_all({})
    

# Unit tests begin
@with_setup(_setup, _teardown)
def test_original_value_without_overrides():
    assert_equals(original_value, my_config())
    

@with_setup(_setup, _teardown)
def test_original_value_with_overrides():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert_equals(new_value, my_config())
    

@with_setup(_setup, _teardown)
def test_original_values_when_others_override():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert_equals(new_value, my_config())
    assert_equals(original_value, my_config_2())
    
    
@with_setup(_setup, _teardown)
def test_resetting_values_when_others_override():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert_equals(new_value, my_config())
    assert_equals(original_value, my_config_2())
    
    # Reset
    module_override_all({})
    assert_equals(original_value, my_config())
    assert_equals(original_value, my_config_2())
