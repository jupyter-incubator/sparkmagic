from nose.tools import raises, with_setup, assert_equals

import remotespark.connectionstringutil as csutil

connection_string = "url=dns;username=user;password=pass"
host = "dns"

def test_string_to_obj():
    cso = csutil.get_connection_string_elements(connection_string)
    assert_equals(cso.url, host)
    assert_equals(cso.username, "user")
    assert_equals(cso.password, "pass")

def test_obj_to_str():
    cs = csutil.get_connection_string(host, "user", "pass")
    assert_equals(connection_string, cs)