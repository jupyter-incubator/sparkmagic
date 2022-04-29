from nose.tools import assert_equals

from sparkmagic.livyclientlib.configurableretrypolicy import ConfigurableRetryPolicy
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


def test_with_empty_list():
    times = []
    max_retries = 5
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert_equals(5, policy.seconds_to_sleep(0))
    assert_equals(5, policy.seconds_to_sleep(4))
    assert_equals(5, policy.seconds_to_sleep(5))
    assert_equals(5, policy.seconds_to_sleep(6))

    # Check based on retry count
    assert_equals(True, policy.should_retry(500, False, 0))
    assert_equals(True, policy.should_retry(500, False, 4))
    assert_equals(True, policy.should_retry(500, False, 5))
    assert_equals(False, policy.should_retry(500, False, 6))

    # Check based on status code
    assert_equals(False, policy.should_retry(201, False, 0))
    assert_equals(False, policy.should_retry(201, False, 6))

    # Check based on error
    assert_equals(True, policy.should_retry(201, True, 0))
    assert_equals(True, policy.should_retry(201, True, 6))


def test_with_one_element_list():
    times = [2]
    max_retries = 5
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert_equals(2, policy.seconds_to_sleep(0))
    assert_equals(2, policy.seconds_to_sleep(4))
    assert_equals(2, policy.seconds_to_sleep(5))
    assert_equals(2, policy.seconds_to_sleep(6))

    # Check based on retry count
    assert_equals(True, policy.should_retry(500, False, 0))
    assert_equals(True, policy.should_retry(500, False, 4))
    assert_equals(True, policy.should_retry(500, False, 5))
    assert_equals(False, policy.should_retry(500, False, 6))

    # Check based on status code
    assert_equals(False, policy.should_retry(201, False, 0))
    assert_equals(False, policy.should_retry(201, False, 6))

    # Check based on error
    assert_equals(True, policy.should_retry(201, True, 0))
    assert_equals(True, policy.should_retry(201, True, 6))


def test_with_default_values():
    times = conf.retry_seconds_to_sleep_list()
    max_retries = conf.configurable_retry_policy_max_retries()
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert_equals(times[0], policy.seconds_to_sleep(0))
    assert_equals(times[0], policy.seconds_to_sleep(1))
    assert_equals(times[1], policy.seconds_to_sleep(2))
    assert_equals(times[2], policy.seconds_to_sleep(3))
    assert_equals(times[3], policy.seconds_to_sleep(4))
    assert_equals(times[4], policy.seconds_to_sleep(5))
    assert_equals(times[4], policy.seconds_to_sleep(6))
    assert_equals(times[4], policy.seconds_to_sleep(7))
    assert_equals(times[4], policy.seconds_to_sleep(8))
    assert_equals(times[4], policy.seconds_to_sleep(9))

    # Check based on retry count
    assert_equals(True, policy.should_retry(500, False, 0))
    assert_equals(True, policy.should_retry(500, False, 7))
    assert_equals(True, policy.should_retry(500, False, 8))
    assert_equals(False, policy.should_retry(500, False, 9))

    # Check based on status code
    assert_equals(False, policy.should_retry(201, False, 0))
    assert_equals(False, policy.should_retry(201, False, 9))

    # Check based on error
    assert_equals(True, policy.should_retry(201, True, 0))
    assert_equals(True, policy.should_retry(201, True, 9))


def test_with_negative_values():
    times = [0.1, -1]
    max_retries = 5

    try:
        policy = ConfigurableRetryPolicy(times, max_retries)
        assert False
    except BadUserConfigurationException:
        assert True
