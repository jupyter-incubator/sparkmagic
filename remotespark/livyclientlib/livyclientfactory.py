# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from .log import Log
from .utils import get_connection_string_elements
from .livysession import LivySession
from .pandaspysparklivyclient import PandasPysparkLivyClient
from .pandasscalalivyclient import PandasScalaLivyClient
from .livyreliablehttpclient import LivyReliableHttpClient
from .constants import Constants
from .linearretrypolicy import LinearRetryPolicy


class LivyClientFactory(object):
    """Spark client for Livy endpoint"""
    logger = Log()
    max_results = 2500

    @staticmethod
    def build_client(language, session):
        assert session is not None

        if language == Constants.lang_python:
            return PandasPysparkLivyClient(session, LivyClientFactory.max_results)
        elif language == Constants.lang_scala:
            return PandasScalaLivyClient(session, LivyClientFactory.max_results)
        else:
            raise ValueError("Language '{}' is not supported.".format(language))

    @staticmethod
    def create_session(language, connection_string, session_id="-1", sql_created=False):
        cso = get_connection_string_elements(connection_string)

        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        http_client = LivyReliableHttpClient(cso.url, cso.username, cso.password, retry_policy)

        session = LivySession(http_client, language, session_id, sql_created)

        return session
