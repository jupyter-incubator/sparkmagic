# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json
from time import sleep
import requests
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from .exceptions import HttpClientException

class ReliableHttpClient(object):
    """Http client that is reliable in its requests. Uses requests library."""

    def __init__(self, endpoint, headers, retry_policy):
        self._endpoint = endpoint
        self._headers = headers
        self._retry_policy = retry_policy
        self._auth = self._endpoint.auth
        self._session = requests.Session()
        self.logger = SparkLog(u"ReliableHttpClient")
        self.verify_ssl = not conf.ignore_ssl_errors()
        if not self.verify_ssl:
            self.logger.debug(u"ATTENTION: Will ignore SSL errors. This might render you vulnerable to attacks.")
            requests.packages.urllib3.disable_warnings()

    def get_headers(self):
        return self._headers

    def compose_url(self, relative_url):
        r_u = "/{}".format(relative_url.rstrip(u"/").lstrip(u"/"))
        return self._endpoint.url + r_u

    def get(self, relative_url, accepted_status_codes):
        """Sends a get request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, self._session.get)

    def post(self, relative_url, accepted_status_codes, data):
        """Sends a post request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, self._session.post, data)

    def delete(self, relative_url, accepted_status_codes):
        """Sends a delete request. Returns a response."""
        return self._send_request(relative_url, accepted_status_codes, self._session.delete)

    def _send_request(self, relative_url, accepted_status_codes, function, data=None):
        return self._send_request_helper(self.compose_url(relative_url), accepted_status_codes, function, data, 0)

    def _send_request_helper(self, url, accepted_status_codes, function, data, retry_count):
        while True:
            try:
                if data is None:
                    r = function(url, headers=self._headers, auth=self._auth, verify=self.verify_ssl)
                else:
                    r = function(url, headers=self._headers, auth=self._auth,
                                    data=json.dumps(data), verify=self.verify_ssl)
            except requests.exceptions.RequestException as e:
                error = True
                r = None
                status = None
                text = None

                self.logger.error(u"Request to '{}' failed with '{}'".format(url, e))
            else:
                error = False
                status = r.status_code
                text = r.text

            if error or status not in accepted_status_codes:
                if self._retry_policy.should_retry(status, error, retry_count):
                    sleep(self._retry_policy.seconds_to_sleep(retry_count))
                    retry_count += 1
                    continue

                if error:
                    raise HttpClientException(u"Error sending http request and maximum retry encountered.")
                else:
                    raise HttpClientException(u"Invalid status code '{}' from {} with error payload: {}"
                                              .format(status, url, text))
            return r
