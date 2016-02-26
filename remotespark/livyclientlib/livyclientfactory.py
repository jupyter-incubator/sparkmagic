# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.utils.constants import SESSION_KIND_PYSPARK, SESSION_KIND_SPARK, SESSION_KIND_SPARKR
import remotespark.utils.configuration as conf
from remotespark.utils.log import Log
from remotespark.utils.utils import get_connection_string_elements
from .linearretrypolicy import LinearRetryPolicy
from .livyreliablehttpclient import LivyReliableHttpClient
from .livysession import LivySession
from .pysparklivyclient import PysparkLivyClient
from .scalalivyclient import ScalaLivyClient
from .rlivyclient import RLivyClient


class LivyClientFactory(object):
    """Spark client factory"""

    def __init__(self):
        self.logger = Log("LivyClientFactory")
        self.max_results = conf.default_maxrows()

    def build_client(self, session):
        assert session is not None
        kind = session.kind

        if kind == SESSION_KIND_PYSPARK:
            return PysparkLivyClient(session)
        elif kind == SESSION_KIND_SPARK:
            return ScalaLivyClient(session)
        elif kind == SESSION_KIND_SPARKR:
            return RLivyClient(session)
        else:
            raise ValueError("Kind '{}' is not supported.".format(kind))

    @staticmethod
    def create_session(ipython_display, connection_string, properties, session_id="-1", sql_created=False):
        http_client = LivyClientFactory.create_http_client(connection_string)

        session = LivySession(ipython_display, http_client, session_id, sql_created, properties)

        return session

    @staticmethod
    def create_http_client(connection_string):
        cso = get_connection_string_elements(connection_string)

        retry_policy = LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        return LivyReliableHttpClient(cso.url, cso.username, cso.password, retry_policy)
