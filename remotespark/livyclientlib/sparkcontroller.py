# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.log import Log
from remotespark.utils.sparkevents import SparkEvents
from .sessionmanager import SessionManager
from .livyreliablehttpclient import LivyReliableHttpClient
from .livysession import LivySession


class SparkController(object):

    def __init__(self, ipython_display):
        self.logger = Log("SparkController")
        self.ipython_display = ipython_display
        self.session_manager = SessionManager()

    def get_logs(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_logs()

    def run_command(self, command, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return command.execute(session_to_use)

    def run_sqlquery(self, sqlquery, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return sqlquery.execute(session_to_use)

    def get_all_sessions_endpoint(self, connection_string):
        http_client = self._http_client_from_connection_string(connection_string)
        sessions = http_client.get_sessions()["sessions"]
        session_list = [self._create_livy_session(connection_string, {"kind": s["kind"]},
                                                  self.ipython_display, s["id"])
                        for s in sessions]
        for s in session_list:
            s._refresh_status()
        return session_list

    def get_all_sessions_endpoint_info(self, connection_string):
        sessions = self.get_all_sessions_endpoint(connection_string)
        return [str(s) for s in sessions]

    def cleanup(self):
        self.session_manager.clean_up_all()

    def cleanup_endpoint(self, connection_string):
        for session in self.get_all_sessions_endpoint(connection_string):
            session.delete()

    def delete_session_by_name(self, name):
        self.session_manager.delete_client(name)

    def delete_session_by_id(self, connection_string, session_id):
        http_client = self._http_client_from_connection_string(connection_string)
        try:
            response = http_client.get_session(session_id)
            session = self._create_livy_session(connection_string, {"kind": response["kind"]},
                                                self.ipython_display, session_id, False)
            session.delete()
        except ValueError:
            # Session does not exist; do nothing
            pass

    def add_session(self, name, connection_string, skip_if_exists, properties):
        if skip_if_exists and (name in self.session_manager.get_sessions_list()):
            self.logger.debug("Skipping {} because it already exists in list of sessions.".format(name))
            return

        session = self._create_livy_session(connection_string, properties, self.ipython_display)
        self.session_manager.add_session(name, session)
        session.start()

    def get_session_id_for_client(self, name):
        return self.session_manager.get_session_id_for_client(name)

    def get_client_keys(self):
        return self.session_manager.get_sessions_list()

    def get_manager_sessions_str(self):
        return self.session_manager.get_sessions_info()

    def get_session_by_name_or_default(self, client_name):
        if client_name is None:
            return self.session_manager.get_any_session()
        else:
            client_name = client_name.lower()
            return self.session_manager.get_session(client_name)

    def get_managed_clients(self):
        return self.session_manager.sessions

    @staticmethod
    def _create_livy_session(*args, **kwargs):
        return LivySession.from_connection_string(*args, **kwargs)

    @staticmethod
    def _http_client_from_connection_string(connection_string):
        return LivyReliableHttpClient.from_connection_string(connection_string)
