# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.utils.constants import Constants
from remotespark.utils.log import Log
from remotespark.utils.utils import get_connection_string_elements
from .linearretrypolicy import LinearRetryPolicy
from .livyreliablehttpclient import LivyReliableHttpClient
from .livysession import LivySession
from .pandaspysparklivyclient import PandasPysparkLivyClient
from .pandasscalalivyclient import PandasScalaLivyClient


class LivyClientFactory(object):
    """Spark client factory"""

    def __init__(self):
        self.logger = Log("LivyClientFactory")
        self.max_results = 2500

    def build_client(self, language, session):
        assert session is not None

        if language == Constants.lang_python:
            return PandasPysparkLivyClient(session, self.max_results)
        elif language == Constants.lang_scala:
            return PandasScalaLivyClient(session, self.max_results)
        else:
            raise ValueError("Language '{}' is not supported.".format(language))

    @staticmethod
    def create_session(language, connection_string, session_id="-1", sql_created=False):
        cso = get_connection_string_elements(connection_string)

        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        http_client = LivyReliableHttpClient(cso.url, cso.username, cso.password, retry_policy)

        session = LivySession(http_client, language, session_id, sql_created)

        return session
