# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class LinearRetryPolicy(object):
    """Retry policy that always returns the same number of seconds to sleep between calls,
    takes all status codes 500 or above to be retriable, and retries a given maximum number of times."""

    def __init__(self, seconds_to_sleep, max_retries):
        self._seconds_to_sleep = seconds_to_sleep
        self.max_retries = max_retries

    def should_retry(self, status_code, error, retry_count):
        if None in (status_code, retry_count):
            return False
        return (status_code >= 500 and retry_count <= self.max_retries) or error

    def seconds_to_sleep(self, retry_count):
        return self._seconds_to_sleep
