from nose.tools import assert_equals

from remotespark.livyclientfactory import LivyClientFactory

headers = {"Content-Type": "application/json", "Authorization": "Basic dXNlcjpwYXNzd29yZA=="}
username = "user"
password = "password"


def test_get_headers():
    factory = LivyClientFactory()
    generated_headers = factory._get_headers(username, password)
    assert_equals(headers, generated_headers)
