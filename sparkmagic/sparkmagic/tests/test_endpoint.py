from nose.tools import assert_equals, assert_not_equal, assert_false

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.auth.basic import Basic
from sparkmagic.auth.kerberos import Kerberos


def test_equality():
    basic_auth1 = Basic()
    basic_auth2 = Basic()
    kerberos_auth1 = Kerberos()
    kerberos_auth2 = Kerberos()
    assert_equals(
        Endpoint("http://url.com", basic_auth1), Endpoint("http://url.com", basic_auth2)
    )
    assert_equals(
        Endpoint("http://url.com", kerberos_auth1),
        Endpoint("http://url.com", kerberos_auth2),
    )


def test_inequality():
    basic_auth1 = Basic()
    basic_auth2 = Basic()
    basic_auth1.username = "user"
    basic_auth2.username = "different_user"
    assert_not_equal(
        Endpoint("http://url.com", basic_auth1), Endpoint("http://url.com", basic_auth2)
    )


def test_invalid_url():
    basic_auth = Basic()
    try:
        endpoint = Endpoint(None, basic_auth)
        assert False
    except BadUserDataException:
        assert True
