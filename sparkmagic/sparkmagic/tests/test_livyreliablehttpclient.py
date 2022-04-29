from mock import MagicMock
from nose.tools import assert_equals

from sparkmagic.livyclientlib.livyreliablehttpclient import LivyReliableHttpClient
from sparkmagic.livyclientlib.endpoint import Endpoint
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.livyclientlib.configurableretrypolicy import ConfigurableRetryPolicy
from sparkmagic.livyclientlib.linearretrypolicy import LinearRetryPolicy


def test_post_statement():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    data = {"adlfj": "sadflkjsdf"}
    out = livy_client.post_statement(100, data)
    assert_equals(out, http_client.post.return_value.json.return_value)
    http_client.post.assert_called_once_with("/sessions/100/statements", [201], data)


def test_get_statement():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    out = livy_client.get_statement(100, 4)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/100/statements/4", [200])


def test_cancel_statement():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    out = livy_client.cancel_statement(100, 104)
    assert_equals(out, http_client.post.return_value.json.return_value)
    http_client.post.assert_called_once_with(
        "/sessions/100/statements/104/cancel", [200], {}
    )


def test_get_sessions():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    out = livy_client.get_sessions()
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions", [200])


def test_post_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    properties = {"adlfj": "sadflkjsdf", 1: [2, 3, 4, 5]}
    out = livy_client.post_session(properties)
    assert_equals(out, http_client.post.return_value.json.return_value)
    http_client.post.assert_called_once_with("/sessions", [201], properties)


def test_get_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    out = livy_client.get_session(4)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/4", [200])


def test_delete_session():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    livy_client.delete_session(99)
    http_client.delete.assert_called_once_with("/sessions/99", [200, 404])


def test_get_all_session_logs():
    http_client = MagicMock()
    livy_client = LivyReliableHttpClient(http_client, None)
    out = livy_client.get_all_session_logs(42)
    assert_equals(out, http_client.get.return_value.json.return_value)
    http_client.get.assert_called_once_with("/sessions/42/log?from=0", [200])


def test_custom_headers():
    custom_headers = {"header1": "value1"}
    overrides = {conf.custom_headers.__name__: custom_headers}
    conf.override_all(overrides)
    endpoint = Endpoint("http://url.com", None)
    client = LivyReliableHttpClient.from_endpoint(endpoint)
    headers = client.get_headers()
    assert_equals(len(headers), 2)
    assert_equals("Content-Type" in headers, True)
    assert_equals("header1" in headers, True)


def test_retry_policy():
    # Default is configurable retry
    times = conf.retry_seconds_to_sleep_list()
    max_retries = conf.configurable_retry_policy_max_retries()
    policy = LivyReliableHttpClient._get_retry_policy()
    assert type(policy) is ConfigurableRetryPolicy
    assert_equals(times, policy.retry_seconds_to_sleep_list)
    assert_equals(max_retries, policy.max_retries)

    # Configure to linear retry
    _override_policy(constants.LINEAR_RETRY)
    policy = LivyReliableHttpClient._get_retry_policy()
    assert type(policy) is LinearRetryPolicy
    assert_equals(5, policy.seconds_to_sleep(1))
    assert_equals(5, policy.max_retries)

    # Configure to something invalid
    _override_policy("garbage")
    try:
        policy = LivyReliableHttpClient._get_retry_policy()
        assert False
    except BadUserConfigurationException:
        assert True


def _override_policy(policy):
    overrides = {conf.retry_policy.__name__: policy}
    conf.override_all(overrides)
