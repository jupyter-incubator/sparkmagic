"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .clientmanager import ClientManager
from .log import Log
from .livyclientfactory import LivyClientFactory
from .filesystemreaderwriter import FileSystemReaderWriter
from .clientmanagerstateserializer import ClientManagerStateSerializer
from .constants import Constants


class SparkController(object):

    def __init__(self, serialize_path=None):
        self.logger = Log("SparkController")
        self.client_factory = LivyClientFactory()

        if serialize_path is not None:
            serializer = ClientManagerStateSerializer(self.client_factory, FileSystemReaderWriter(serialize_path))
            self.client_manager = ClientManager(serializer)
        else:
            self.client_manager = ClientManager()

    def run_cell(self, client_name, context, cell):
        context = context.lower()

        # Select client
        if client_name is None:
            client_to_use = self.client_manager.get_any_client()
        else:
            client_name = client_name.lower()
            client_to_use = self.client_manager.get_client(client_name)

        # Execute in context
        if context == Constants.context_name_sql:
            res = client_to_use.execute_sql(cell)
        elif context == Constants.context_name_hive:
            res = client_to_use.execute_hive(cell)
        elif context == Constants.context_name_spark:
            res = client_to_use.execute(cell)
        else:
            raise ValueError("Context '{}' specified is not known.")

        return res

    def cleanup(self):
        self.client_manager.clean_up_all()

    def delete_endpoint(self, name):
        self.client_manager.delete_client(name)

    def add_endpoint(self, name, language, connection_string, skip_if_exists):
        if skip_if_exists and (name in self.client_manager.get_endpoints_list()):
            self.logger.debug("Skipping {} because it already exists in list of endpoints.".format(name))
            return

        session = self.client_factory.create_session(language, connection_string, "-1", False)
        session.start()
        livy_client = self.client_factory.build_client(language, session)
        self.client_manager.add_client(name, livy_client)

    def get_client_keys(self):
        return self.client_manager.get_endpoints_list()
