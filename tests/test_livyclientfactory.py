from nose.tools import assert_equals

from remotespark.livyclientlib.livyclientfactory import LivyClientFactory

headers = {"Content-Type": "application/json"}


def test_get_headers():
    factory = LivyClientFactory()
    generated_headers = factory._get_headers()
    assert_equals(headers, generated_headers)
