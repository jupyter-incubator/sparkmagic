# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.utils import get_connection_string_elements
from .linearretrypolicy import LinearRetryPolicy
from .reliablehttpclient import ReliableHttpClient


class LivyReliableHttpClient(ReliableHttpClient):
    """Default headers."""

    def __init__(self, url, username, password, retry_policy):
        super(LivyReliableHttpClient, self).__init__(url, {"Content-Type": "application/json"},
                                                     username, password, retry_policy)

    @staticmethod
    def from_connection_string(connection_string):
        cso = get_connection_string_elements(connection_string)

        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        return LivyReliableHttpClient(cso.url, cso.username, cso.password, retry_policy)
