"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from IPython.core.getipython import get_ipython

from .clientmanager import ClientManager
from .log import Log
from .livyclientfactory import LivyClientFactory
from .filesystemreaderwriter import FileSystemReaderWriter
from .clientmanagerstateserializer import ClientManagerStateSerializer
from .constants import Constants
from .dataframeparseexception import DataFrameParseException

class SparkController(object):

    def __init__(self, serialize_path=None):
        self.logger = Log("SparkController")
        self.client_factory = LivyClientFactory()
        self.ipython = get_ipython()

        if serialize_path is not None:
            serializer = ClientManagerStateSerializer(self.client_factory, FileSystemReaderWriter(serialize_path))
            self.client_manager = ClientManager(serializer)
        else:
            self.client_manager = ClientManager()

    def run_cell(self, cell, client_name = None):
        client_to_use = self.get_client_by_name(client_name)
        (success, out) = client_to_use.execute(cell)
        if success:
            self.ipython.write(out)
        else:
            self.ipython.write_err(out)

    def run_cell_sql(self, cell, client_name = None):
        client_to_use = self.get_client_by_name(client_name)
        try:
            return client_to_use.execute_sql(cell)
        except DataFrameParseException as e:
            self.ipython.write_err(e.out)

    def run_cell_hive(self, cell, client_name = None):
        client_to_use = self.get_client_by_name(client_name)
        try:
            return client_to_use.execute_hive(cell)
        except DataFrameParseException as e:
            self.ipython.write_err(e.out)

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

    def get_client_by_name(self, client_name = None):
        if client_name is None:
            return self.client_manager.get_any_client()
        else:
            client_name = client_name.lower()
            return self.client_manager.get_client(client_name)
        
