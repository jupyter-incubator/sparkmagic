# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .linearretrypolicy import LinearRetryPolicy
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


class ConfigurableRetryPolicy(LinearRetryPolicy):
    """Retry policy that returns a configurable number of seconds to sleep between calls,
    takes all status codes 500 or above to be retriable, and retries a given maximum number of times.
    If the retry count exceeds the number of items in the list, last item in the list is always returned."""

    def __init__(self, retry_seconds_to_sleep_list, max_retries):
        super(ConfigurableRetryPolicy, self).__init__(-1, max_retries)

        # If user configured to an empty list, let's make this behave as
        # a Linear Retry Policy by assigning a list of 1 element.
        if len(retry_seconds_to_sleep_list) == 0:
            retry_seconds_to_sleep_list = [5]
        elif not all(n > 0 for n in retry_seconds_to_sleep_list):
            raise BadUserConfigurationException(u"All items in the list in your config need to be positive for configurable retry policy")

        self.retry_seconds_to_sleep_list = retry_seconds_to_sleep_list
        self._max_index = len(self.retry_seconds_to_sleep_list) - 1

    def seconds_to_sleep(self, retry_count):
        index = max(retry_count - 1, 0)
        if index > self._max_index:
            index = self._max_index

        return self.retry_seconds_to_sleep_list[index]
