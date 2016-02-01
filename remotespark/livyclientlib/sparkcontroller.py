# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.filesystemreaderwriter import FileSystemReaderWriter
from remotespark.utils.log import Log
from .clientmanager import ClientManager
from .clientmanagerstateserializer import ClientManagerStateSerializer
from .livyclientfactory import LivyClientFactory


class SparkController(object):

    def __init__(self, ipython_display, serialize_path=None):
        self.logger = Log("SparkController")
        self.ipython_display = ipython_display
        self.client_factory = LivyClientFactory()

        if serialize_path is not None:
            serializer = ClientManagerStateSerializer(self.client_factory, FileSystemReaderWriter(serialize_path))
            self.client_manager = ClientManager(serializer)
        else:
            self.client_manager = ClientManager()

    def get_logs(self, client_name=None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.get_logs()

    def run_cell(self, cell, client_name=None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute(cell)

    def run_cell_sql(self, cell, client_name=None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute_sql(cell)

    def run_cell_hive(self, cell, client_name=None):
        client_to_use = self.get_client_by_name_or_default(client_name)
        return client_to_use.execute_hive(cell)

    def get_all_sessions_endpoint(self, connection_string):
        http_client = self.client_factory.create_http_client(connection_string)
        r = http_client.get("/sessions", [200])
        sessions = r.json()["sessions"]
        session_list = [self.client_factory.create_session(self.ipython_display, connection_string, {"kind": s["kind"]}, s["id"])
                        for s in sessions]
        for s in session_list:
            s._refresh_status()
        return session_list

    def get_all_sessions_endpoint_info(self, connection_string):
        sessions = self.get_all_sessions_endpoint(connection_string)
        return [str(s) for s in sessions]

    def cleanup(self):
        self.client_manager.clean_up_all()

    def cleanup_endpoint(self, connection_string):
        for session in self.get_all_sessions_endpoint(connection_string):
            session.delete()

    def delete_session_by_name(self, name):
        self.client_manager.delete_client(name)

    def delete_session_by_id(self, connection_string, session_id):
        http_client = self.client_factory.create_http_client(connection_string)
        r = http_client.get("/sessions/{}".format(session_id), [200, 404])
        if r.status_code != 404:
            session = self.client_factory.create_session(self.ipython_display, connection_string, {"kind": r.json()["kind"]}, session_id, False)
            session.delete()

    def add_session(self, name, connection_string, skip_if_exists, properties):
        if skip_if_exists and (name in self.client_manager.get_sessions_list()):
            self.logger.debug("Skipping {} because it already exists in list of sessions.".format(name))
            return

        session = self.client_factory.create_session(self.ipython_display, connection_string, properties, "-1", False)
        session.start()

        livy_client = self.client_factory.build_client(session)
        self.client_manager.add_client(name, livy_client)
        livy_client.start()

    def get_session_id_for_client(self, name):
        return self.client_manager.get_session_id_for_client(name)

    def get_client_keys(self):
        return self.client_manager.get_sessions_list()

    def get_manager_sessions_str(self):
        return self.client_manager.get_sessions_info()

    def get_client_by_name_or_default(self, client_name):
        if client_name is None:
            return self.client_manager.get_any_client()
        else:
            client_name = client_name.lower()
            return self.client_manager.get_client(client_name)

    def get_managed_clients(self):
        return self.client_manager.livy_clients
