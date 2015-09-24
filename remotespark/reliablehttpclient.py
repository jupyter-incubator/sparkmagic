# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json
import requests


class ReliableHttpClient(object):
    """Http client that is reliable in its requests. Uses requests library."""
    # TODO(aggftw): retry on retriable status codes
    # TODO(aggftw): unit tests

    def __init__(self, url, headers):
        self._url = url.rstrip("/")
        self._headers = headers

    def compose_url(self, relative_url):
        r_u = "/{}".format(relative_url.rstrip("/").lstrip("/"))
        return self._url + r_u

    def get(self, relative_url, accepted_status_codes):
        """Sends a get request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, requests.get)

    def post(self, relative_url, accepted_status_codes, data):
        """Sends a post request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, requests.post, data)

    def delete(self, relative_url, accepted_status_codes):
        """Sends a delete request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, requests.delete)

    def _send_request(self, relative_url, accepted_status_codes, function, data=None):
        """Sends a delete request. Returns a response."""
        url = self.compose_url(relative_url)

        if data is None:
            r = function(url, headers=self._headers)
        else:
            r = function(url, headers=self._headers, data=json.dumps(data))

        if r.status_code not in accepted_status_codes:
            raise ValueError("Invalid status code '{}' from {}"
                             .format(r.status_code, url))
        return r
