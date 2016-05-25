from mock import MagicMock
from nose.tools import assert_equals

from sparkmagic.livyclientlib.livyreliablehttpclient import LivyReliableHttpClient


def test_post_statement():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    data = {"adlfj":"sadflkjsdf"}
    out = livy_client.post_statement(100, data)
    assert_equals(out, http_client.post.return_value.json.return_value)
    http_client.post.assert_called_once_with("/sessions/100/statements", [201], data)


def test_get_statement():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    out = livy_client.get_statement(100, 4)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/100/statements/4", [200])


def test_get_sessions():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    out = livy_client.get_sessions()
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions", [200])


def test_post_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    properties = {"adlfj":"sadflkjsdf", 1: [2,3,4,5]}
    out = livy_client.post_session(properties)
    assert_equals(out, http_client.post.return_value.json.return_value)
    http_client.post.assert_called_once_with("/sessions", [201], properties)


def test_get_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    out = livy_client.get_session(4)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/4", [200])


def test_delete_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    livy_client.delete_session(99)
    http_client.delete.assert_called_once_with("/sessions/99", [200, 404])


def test_get_all_session_logs():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client)
    out = livy_client.get_all_session_logs(42)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/42/log?from=0", [200])

