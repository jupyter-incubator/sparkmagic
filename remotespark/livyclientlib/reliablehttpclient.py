# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json
from time import sleep

import requests

import remotespark.utils.configuration as conf
from remotespark.utils.log import Log
from remotespark.utils.utils import get_connection_string


class ReliableHttpClient(object):
    """Http client that is reliable in its requests. Uses requests library."""

    def __init__(self, url, headers, username, password, retry_policy):
        self._url = url.rstrip("/")
        self._headers = headers
        self._username = username
        self._password = password
        self._retry_policy = retry_policy
        self.logger = Log("ReliableHttpClient")

        self._do_not_authenticate = self._username == "" and self._password == ""

        self.verify_ssl = not conf.ignore_ssl_errors()
        if not self.verify_ssl:
            self.logger.debug("ATTENTION: Will ignore SSL errors. This might render you vulnerable to attacks.")

    @property
    def connection_string(self):
        return get_connection_string(self._url, self._username, self._password)

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
        return self._send_request_helper(self.compose_url(relative_url), accepted_status_codes, function, data, 0)

    def _send_request_helper(self, url, accepted_status_codes, function, data, retry_count):

        try:
            if self._do_not_authenticate:
                if data is None:
                    r = function(url, headers=self._headers, verify=self.verify_ssl)
                else:
                    r = function(url, headers=self._headers, data=json.dumps(data), verify=self.verify_ssl)
            else:
                if data is None:
                    r = function(url, headers=self._headers, auth=(self._username, self._password),
                                 verify=self.verify_ssl)
                else:
                    r = function(url, headers=self._headers, auth=(self._username, self._password), data=json.dumps(data),
                                 verify=self.verify_ssl)
        except requests.exceptions.RequestException as e:
            error = True
            r = None
            status = None

            self.logger.error("Request to '{}' failed with '{}'".format(url, e))
        else:
            error = False
            status = r.status_code

        if error or status not in accepted_status_codes:
            if self._retry_policy.should_retry(status, error, retry_count):
                sleep(self._retry_policy.seconds_to_sleep(retry_count))
                return self._send_request_helper(url, accepted_status_codes, function, data, retry_count + 1)
            else:
                raise ValueError("Invalid status code '{}' or error '{}' from {}"
                                 .format(status, error, url))
        return r
