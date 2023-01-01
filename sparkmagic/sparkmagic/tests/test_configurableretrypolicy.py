from sparkmagic.livyclientlib.configurableretrypolicy import ConfigurableRetryPolicy
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


def test_with_empty_list():
    times = []
    max_retries = 5
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert 5 == policy.seconds_to_sleep(0)
    assert 5 == policy.seconds_to_sleep(4)
    assert 5 == policy.seconds_to_sleep(5)
    assert 5 == policy.seconds_to_sleep(6)

    # Check based on retry count
    assert True is policy.should_retry(500, False, 0)
    assert True is policy.should_retry(500, False, 4)
    assert True is policy.should_retry(500, False, 5)
    assert False is policy.should_retry(500, False, 6)

    # Check based on status code
    assert False is policy.should_retry(201, False, 0)
    assert False is policy.should_retry(201, False, 6)

    # Check based on error
    assert True is policy.should_retry(201, True, 0)
    assert True is policy.should_retry(201, True, 6)


def test_with_one_element_list():
    times = [2]
    max_retries = 5
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert 2 == policy.seconds_to_sleep(0)
    assert 2 == policy.seconds_to_sleep(4)
    assert 2 == policy.seconds_to_sleep(5)
    assert 2 == policy.seconds_to_sleep(6)

    # Check based on retry count
    assert True is policy.should_retry(500, False, 0)
    assert True is policy.should_retry(500, False, 4)
    assert True is policy.should_retry(500, False, 5)
    assert False is policy.should_retry(500, False, 6)

    # Check based on status code
    assert False is policy.should_retry(201, False, 0)
    assert False is policy.should_retry(201, False, 6)

    # Check based on error
    assert True is policy.should_retry(201, True, 0)
    assert True is policy.should_retry(201, True, 6)


def test_with_default_values():
    times = conf.retry_seconds_to_sleep_list()
    max_retries = conf.configurable_retry_policy_max_retries()
    policy = ConfigurableRetryPolicy(times, max_retries)

    assert times[0] == policy.seconds_to_sleep(0)
    assert times[0] == policy.seconds_to_sleep(1)
    assert times[1] == policy.seconds_to_sleep(2)
    assert times[2] == policy.seconds_to_sleep(3)
    assert times[3] == policy.seconds_to_sleep(4)
    assert times[4] == policy.seconds_to_sleep(5)
    assert times[4] == policy.seconds_to_sleep(6)
    assert times[4] == policy.seconds_to_sleep(7)
    assert times[4] == policy.seconds_to_sleep(8)
    assert times[4] == policy.seconds_to_sleep(9)

    # Check based on retry count
    assert True is policy.should_retry(500, False, 0)
    assert True is policy.should_retry(500, False, 7)
    assert True is policy.should_retry(500, False, 8)
    assert False is policy.should_retry(500, False, 9)

    # Check based on status code
    assert False is policy.should_retry(201, False, 0)
    assert False is policy.should_retry(201, False, 9)

    # Check based on error
    assert True is policy.should_retry(201, True, 0)
    assert True is policy.should_retry(201, True, 9)


def test_with_negative_values():
    times = [0.1, -1]
    max_retries = 5

    try:
        policy = ConfigurableRetryPolicy(times, max_retries)
        assert False
    except BadUserConfigurationException:
        assert True
