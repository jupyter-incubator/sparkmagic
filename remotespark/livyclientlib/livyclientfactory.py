# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from .log import Log
from .connectionstringutil import get_connection_string_elements
from .livysession import LivySession
from .pandaspysparklivyclient import PandasPysparkLivyClient
from .pandasscalalivyclient import PandasScalaLivyClient
from .reliablehttpclient import ReliableHttpClient
from .constants import Constants
from .linearretrypolicy import LinearRetryPolicy


class LivyClientFactory(object):
    """Spark client for Livy endpoint"""
    logger = Log()
    max_results = 2500

    def __init__(self):
        pass

    def build_client(self, connection_string, language):
        cso = get_connection_string_elements(connection_string)

        headers = self._get_headers()

        # 30 seconds on a request max
        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)

        http_client = ReliableHttpClient(cso.url, headers, cso.username, cso.password, retry_policy)

        session = self._create_session(http_client, language)

        if language == Constants.lang_python:
            return PandasPysparkLivyClient(session, self.max_results)
        elif language == Constants.lang_scala:
            return PandasScalaLivyClient(session, self.max_results)
        else:
            raise ValueError("Language '{}' is not supported.".format(language))

    @staticmethod
    def _create_session(http_client, language):
        session = LivySession(http_client, language)
        session.start()
        return session

    @staticmethod
    def _get_headers():
        return {"Content-Type": "application/json"}
