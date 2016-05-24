# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .linearretrypolicy import LinearRetryPolicy
from .reliablehttpclient import ReliableHttpClient


class LivyReliableHttpClient(object):
    """A Livy-specific Http client which wraps the normal ReliableHttpClient. Propagates
    HttpClientExceptions up."""
    def __init__(self, http_client):
        self._http_client = http_client

    @staticmethod
    def from_endpoint(endpoint):
        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        return LivyReliableHttpClient(ReliableHttpClient(endpoint, {"Content-Type": "application/json"},
                                                         retry_policy))

    def post_statement(self, session_id, data):
        return self._http_client.post(self._statements_url(session_id), [201], data).json()

    def get_statement(self, session_id, statement_id):
        return self._http_client.get(self._statement_url(session_id, statement_id), [200]).json()

    def get_sessions(self):
        return self._http_client.get("/sessions", [200]).json()

    def post_session(self, properties):
        return self._http_client.post("/sessions", [201], properties).json()

    def get_session(self, session_id):
        return self._http_client.get(self._session_url(session_id), [200]).json()

    def delete_session(self, session_id):
        self._http_client.delete(self._session_url(session_id), [200, 404])

    def get_all_session_logs(self, session_id):
        return self._http_client.get(self._session_url(session_id) + "/log?from=0", [200]).json()

    @staticmethod
    def _session_url(session_id):
        return "/sessions/{}".format(session_id)

    @staticmethod
    def _statements_url(session_id):
        return "/sessions/{}/statements".format(session_id)

    @staticmethod
    def _statement_url(session_id, statement_id):
        return "/sessions/{}/statements/{}".format(session_id, statement_id)
