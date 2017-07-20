from nose.tools import assert_equals

from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.utils.constants import AUTH_BASIC, NO_AUTH


def test_equality():
    assert_equals(Endpoint("http://url.com", AUTH_BASIC, "sdf", "w"), Endpoint("http://url.com", AUTH_BASIC, "sdf", "w"))
    assert_equals(Endpoint("http://url.com", NO_AUTH, "sdf", "w"), Endpoint("http://url.com", NO_AUTH, "sdf", "w"))


def test_invalid_auth():
    try:
        endpoint = Endpoint("http://url.com", "Invalid_AUTH", "username", "password")
        assert False
    except BadUserConfigurationException:
        assert True
