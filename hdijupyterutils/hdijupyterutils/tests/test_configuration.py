from mock import MagicMock

from hdijupyterutils.configuration import override, override_all, with_override


# This is a sample implementation of how a module would use the config methods.
# We'll use these three functions to test it works.
d = {}
path = "~/.testing/config.json"
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
def setup_function():
    module_override_all({})


def teardown_function():
    module_override_all({})


# Unit tests begin
def test_original_value_without_overrides():
    assert original_value == my_config()


def test_original_value_with_overrides():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert new_value == my_config()


def test_original_values_when_others_override():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert new_value == my_config()
    assert original_value == my_config_2()


def test_resetting_values_when_others_override():
    new_value = 2
    module_override(my_config.__name__, new_value)
    assert new_value == my_config()
    assert original_value == my_config_2()

    # Reset
    module_override_all({})
    assert original_value == my_config()
    assert original_value == my_config_2()
