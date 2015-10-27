"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .clientmanager import ClientManager
from .livyclientfactory import LivyClientFactory
from .log import Log


class SparkController(object):

    logger = Log()

    def __init__(self):
        self.client_manager = ClientManager()
        self.client_factory = LivyClientFactory()

    @staticmethod
    def get_log_mode():
        return Log.mode

    @staticmethod
    def set_log_mode(mode):
        Log.mode = mode

    def run_cell(self, client_name, sql, cell):
        # Select client
        if client_name is None:
            client_to_use = self.client_manager.get_any_client()
        else:
            client_name = client_name.lower()
            client_to_use = self.client_manager.get_client(client_name)

        # Execute
        if sql:
            res = client_to_use.execute_sql(cell)
        else:
            res = client_to_use.execute(cell)

        return res

    def cleanup(self):
        self.client_manager.clean_up_all()

    def delete_endpoint(self, name):
        self.client_manager.delete_client(name)

    def add_endpoint(self, name, language, connection_string):
        session = self.client_factory.create_session(language, connection_string, "-1", False)
        session.start()
        livy_client = self.client_factory.build_client(language, session)
        self.client_manager.add_client(name, livy_client)

    def get_client_keys(self):
        return self.client_manager.get_endpoints_list()
