# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from .log import Log
from .connectionstringutil import get_connection_string_elements
from .livysession import LivySession
from .livyclient import LivyClient
from .pandaspysparklivyclient import PandasPysparkLivyClient
from .reliablehttpclient import ReliableHttpClient
from .constants import Constants


class LivyClientFactory(object):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self):
        pass

    def build_client(self, connection_string, language):
        cso = get_connection_string_elements(connection_string)

        headers = self._get_headers()

        http_client = ReliableHttpClient(cso.url, headers, cso.username, cso.password)

        session = self._create_session(http_client, language)

        if language == Constants.lang_python:
            return PandasPysparkLivyClient(session, 250)
        else:
            return LivyClient(session)

    @staticmethod
    def _create_session(http_client, language):
        session = LivySession(http_client, language)
        session.start()
        return session

    @staticmethod
    def _get_headers():
        return {"Content-Type": "application/json"}
