# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from mock import patch, PropertyMock, MagicMock
from nose.tools import (
    raises,
    assert_equals,
    with_setup,
    assert_is_not_none,
    assert_false,
    assert_true,
)
import requests
from requests_kerberos.kerberos_ import HTTPKerberosAuth, REQUIRED, OPTIONAL
from sparkmagic.auth.basic import Basic
from sparkmagic.auth.kerberos import Kerberos
from sparkmagic.auth.customauth import Authenticator
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import HttpClientException
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.livyclientlib.linearretrypolicy import LinearRetryPolicy
from sparkmagic.livyclientlib.reliablehttpclient import ReliableHttpClient
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants

retry_policy = None
sequential_values = []
basic_auth = Basic()
kerberos_auth = Kerberos()
endpoint = Endpoint("http://url.com", basic_auth)


def _setup():
    global retry_policy
    retry_policy = LinearRetryPolicy(0.01, 5)


def _teardown():
    pass


def return_sequential():
    global sequential_values
    val = sequential_values[0]
    sequential_values = sequential_values[1:]
    return val


@with_setup(_setup, _teardown)
def test_compose_url():
    client = ReliableHttpClient(endpoint, {}, retry_policy)

    composed = client.compose_url("r")
    assert_equals("http://url.com/r", composed)

    composed = client.compose_url("/r")
    assert_equals("http://url.com/r", composed)

    client = ReliableHttpClient(endpoint, {}, retry_policy)

    composed = client.compose_url("r")
    assert_equals("http://url.com/r", composed)

    composed = client.compose_url("/r")
    assert_equals("http://url.com/r", composed)


@with_setup(_setup, _teardown)
def test_get():
    with patch("requests.Session.get") as patched_get:
        type(patched_get.return_value).status_code = 200

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)


@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_get_throws():
    with patch("requests.Session.get") as patched_get:
        type(patched_get.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.get("r", [200])


@with_setup(_setup, _teardown)
def test_get_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch("requests.Session.get") as patched_get:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_get.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_post():
    with patch("requests.Session.post") as patched_post:
        type(patched_post.return_value).status_code = 200

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)


@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_post_throws():
    with patch("requests.Session.post") as patched_post:
        type(patched_post.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.post("r", [200], {})


@with_setup(_setup, _teardown)
def test_post_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch("requests.Session.post") as patched_post:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_post.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_delete():
    with patch("requests.Session.delete") as patched_delete:
        type(patched_delete.return_value).status_code = 200

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)


@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_delete_throws():
    with patch("requests.Session.delete") as patched_delete:
        type(patched_delete.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.delete("r", [200])


@with_setup(_setup, _teardown)
def test_delete_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch("requests.Session.delete") as patched_delete:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_delete.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_will_retry_error_no():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = False
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch("requests.Session.get") as patched_get:
        patched_get.side_effect = requests.exceptions.ConnectionError()
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        try:
            client.get("r", [200])
            assert False
        except HttpClientException:
            retry_policy.should_retry.assert_called_once_with(None, True, 0)


@with_setup(_setup, _teardown)
def test_basic_auth_check_auth():
    endpoint = Endpoint("http://url.com", basic_auth)
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert isinstance(client._auth, Basic)
    assert hasattr(client._auth, "username")
    assert hasattr(client._auth, "password")
    assert_equals(client._auth.username, endpoint.auth.username)
    assert_equals(client._auth.password, endpoint.auth.password)


@with_setup(_setup, _teardown)
def test_no_auth_check_auth():
    endpoint = Endpoint("http://url.com", None)
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert_equals(client._auth, None)


@with_setup(_setup, _teardown)
def test_kerberos_auth_check_auth():
    endpoint = Endpoint("http://url.com", kerberos_auth)
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert_is_not_none(client._auth)
    assert isinstance(client._auth, HTTPKerberosAuth)
    assert hasattr(client._auth, "mutual_authentication")
    assert_equals(client._auth.mutual_authentication, REQUIRED)


@with_setup(_setup, _teardown)
def test_kerberos_auth_custom_configuration():
    custom_kerberos_conf = {"mutual_authentication": OPTIONAL, "force_preemptive": True}
    overrides = {conf.kerberos_auth_configuration.__name__: custom_kerberos_conf}
    conf.override_all(overrides)
    kerberos_auth = Kerberos()
    endpoint = Endpoint("http://url.com", kerberos_auth)
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert_is_not_none(client._auth)
    assert isinstance(client._auth, HTTPKerberosAuth)
    assert hasattr(client._auth, "mutual_authentication")
    assert_equals(client._auth.mutual_authentication, OPTIONAL)
    assert hasattr(client._auth, "force_preemptive")
    assert_equals(client._auth.force_preemptive, True)
