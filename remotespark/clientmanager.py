# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from base64 import b64encode

from log import Log
from connectionstringutil import get_connection_string_elements
from livysession import LivySession
from livyclient import LivyClient
from reliablehttpclient import ReliableHttpClient

class ClientManager(object):
    """Livy client manager"""
    logger = Log()

    def __init__(self):
        self.livy_clients = dict()

    def get_endpoints_list(self):
        return self.livy_clients.keys()

    def add_client(self, name, connection_string):
        if name in self.livy_clients.keys():
            raise AssertionError("Endpoint with name '{}' already exists. Please delete the endpoint"
                                 " first if you intend to replace it.".format(name))

        cso = get_connection_string_elements(connection_string)

        token = b64encode(bytes(cso.username + ":" + cso.password)).decode("ascii")
        headers = {"Content-Type": "application/json", "Authorization": "Basic {}".format(token)}

        http_client = ReliableHttpClient(cso.url, headers)

        spark_session = LivySession(http_client, "spark")
        pyspark_session = LivySession(http_client, "pyspark")

        livy_client = LivyClient(spark_session, pyspark_session)

        self.livy_clients[name] = livy_client

    def get_any_client(self):
        number_of_sessions = len(self.livy_clients)
        if number_of_sessions == 1:
            key = self.livy_clients.keys()[0]
            return self.livy_clients[key]
        elif number_of_sessions == 0:
            raise SyntaxError("You need to have at least 1 client created to execute commands.")
        else:
            raise SyntaxError("Please specify the client to use. {}".format(self._get_client_keys()))
        
    def get_client(self, name):
        if name in self.livy_clients.keys():
            return self.livy_clients[name]
        raise ValueError("Could not find '{}' endpoint in list of saved endpoints. {}".format(name, self._get_client_keys()))

    def delete_client(self, name):
        self._remove_endpoint(name)
    
    def clean_up_all(self):
        for name in self.livy_clients.keys():
            self._remove_endpoint(name)

    def _remove_endpoint(self, name):
        if name in self.livy_clients.keys():
            self.livy_clients[name].close_sessions()
            del self.livy_clients[name]
        else:
            raise ValueError("Could not find '{}' endpoint in list of saved endpoints. {}".format(name, self._get_client_keys()))