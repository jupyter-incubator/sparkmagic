# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .reliablehttpclient import ReliableHttpClient


class LivyReliableHttpClient(ReliableHttpClient):
    """Default headers."""

    def __init__(self, url, username, password, retry_policy):
        super(LivyReliableHttpClient, self).__init__(url, {"Content-Type": "application/json"},
                                                     username, password, retry_policy)
