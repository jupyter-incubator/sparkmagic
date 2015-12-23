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

    def build_client(self, session):
        assert session is not None
        kind = session.kind

        if kind == Constants.session_kind_pyspark:
            return PandasPysparkLivyClient(session, self.max_results)
        elif kind == Constants.session_kind_spark:
            return PandasScalaLivyClient(session, self.max_results)
        else:
            raise ValueError("Kind '{}' is not supported.".format(kind))

    @staticmethod
    def create_session(connection_string, properties, session_id="-1", sql_created=False):
        http_client = LivyClientFactory.create_http_client(connection_string)

        session = LivySession(http_client, session_id, sql_created, properties)

        return session

    @staticmethod
    def create_http_client(connection_string):
        cso = get_connection_string_elements(connection_string)

        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        return LivyReliableHttpClient(cso.url, cso.username, cso.password, retry_policy)
