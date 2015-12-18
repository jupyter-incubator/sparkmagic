# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.filesystemreaderwriter import FileSystemReaderWriter
from remotespark.utils.log import Log
from .clientmanager import ClientManager
from .clientmanagerstateserializer import ClientManagerStateSerializer
from .livyclientfactory import LivyClientFactory


class SparkController(object):

    def __init__(self, serialize_path=None):
        self.logger = Log("SparkController")
        self.client_factory = LivyClientFactory()

        if serialize_path is not None:
            serializer = ClientManagerStateSerializer(self.client_factory, FileSystemReaderWriter(serialize_path))
            self.client_manager = ClientManager(serializer)
        else:
            self.client_manager = ClientManager()

    def run_cell(self, cell, client_name = None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute(cell)

    def run_cell_sql(self, cell, client_name = None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute_sql(cell)

    def run_cell_hive(self, cell, client_name = None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute_hive(cell)

    def cleanup(self):
        self.client_manager.clean_up_all()

    def delete_session(self, name):
        self.client_manager.delete_client(name)

    def add_session(self, name, language, connection_string, skip_if_exists):
        if skip_if_exists and (name in self.client_manager.get_sessions_list()):
            self.logger.debug("Skipping {} because it already exists in list of sessions.".format(name))
            return

        session = self.client_factory.create_session(language, connection_string, "-1", False)
        session.start()
        livy_client = self.client_factory.build_client(language, session)
        self.client_manager.add_client(name, livy_client)

    def get_client_keys(self):
        return self.client_manager.get_sessions_list()

    def get_client_by_name_or_default(self, client_name):
        if client_name is None:
            return self.client_manager.get_any_client()
        else:
            client_name = client_name.lower()
            return self.client_manager.get_client(client_name)
        
