# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from nose.tools import raises, assert_equals, with_setup
from mock import patch, PropertyMock, MagicMock

from remotespark.livyclientlib.reliablehttpclient import ReliableHttpClient
from remotespark.livyclientlib.linearretrypolicy import LinearRetryPolicy


retry_policy = None
sequential_values = []


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
    client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

    composed = client.compose_url("r")
    assert_equals("http://url.com/r", composed)

    composed = client.compose_url("/r")
    assert_equals("http://url.com/r", composed)

    client = ReliableHttpClient("http://url.com/", {}, "username", "password", retry_policy)

    composed = client.compose_url("r")
    assert_equals("http://url.com/r", composed)

    composed = client.compose_url("/r")
    assert_equals("http://url.com/r", composed)


@with_setup(_setup, _teardown)
def test_get():
    with patch('requests.get') as patched_get:
        type(patched_get.return_value).status_code = 200

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)


@raises(ValueError)
@with_setup(_setup, _teardown)
def test_get_throws():
    with patch('requests.get') as patched_get:
        type(patched_get.return_value).status_code = 500

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        client.get("r", [200])


@with_setup(_setup, _teardown)
def test_get_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.get') as patched_get:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_get.return_value).status_code = pm
        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_post():
    with patch('requests.post') as patched_post:
        type(patched_post.return_value).status_code = 200

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)


@raises(ValueError)
@with_setup(_setup, _teardown)
def test_post_throws():
    with patch('requests.post') as patched_post:
        type(patched_post.return_value).status_code = 500

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        client.post("r", [200], {})


@with_setup(_setup, _teardown)
def test_post_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.post') as patched_post:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_post.return_value).status_code = pm
        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_delete():
    with patch('requests.delete') as patched_delete:
        type(patched_delete.return_value).status_code = 200

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)


@raises(ValueError)
@with_setup(_setup, _teardown)
def test_delete_throws():
    with patch('requests.delete') as patched_delete:
        type(patched_delete.return_value).status_code = 500

        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        client.delete("r", [200])


@with_setup(_setup, _teardown)
def test_delete_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.delete') as patched_delete:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_delete.return_value).status_code = pm
        client = ReliableHttpClient("http://url.com", {}, "username", "password", retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)
