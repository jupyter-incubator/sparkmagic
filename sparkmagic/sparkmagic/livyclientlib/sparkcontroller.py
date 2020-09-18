# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.utils.sparklogger import SparkLog
from .sessionmanager import SessionManager
from .livyreliablehttpclient import LivyReliableHttpClient
from .livysession import LivySession
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME


class SparkController(object):

    def __init__(self, ipython_display):
        self.logger = SparkLog(u"SparkController")
        self.ipython_display = ipython_display
        self.session_manager = SessionManager(ipython_display)
        # this is to reuse the already created http clients
        # since the reliablehttpclient uses requests session
        self._http_clients = {}

    def get_app_id(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_app_id()

    def get_driver_log_url(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_driver_log_url()

    def get_logs(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_logs()

    def get_spark_ui_url(self, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return session_to_use.get_spark_ui_url()

    def run_command(self, command, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return command.execute(session_to_use)

    def run_sqlquery(self, sqlquery, client_name=None):
        session_to_use = self.get_session_by_name_or_default(client_name)
        return sqlquery.execute(session_to_use)

    def get_all_sessions_endpoint(self, endpoint):
        http_client = self._http_client(endpoint)
        sessions = http_client.get_sessions()[u"sessions"]
        supported_sessions = filter(lambda s: (s[constants.LIVY_KIND_PARAM] in constants.SESSION_KINDS_SUPPORTED), sessions)
        session_list = [self._livy_session(http_client, {constants.LIVY_KIND_PARAM: s[constants.LIVY_KIND_PARAM]},
                                           self.ipython_display, s[u"id"])
                        for s in supported_sessions]
        for s in session_list:
            s.refresh_status_and_info()
        return session_list

    def get_all_sessions_endpoint_info(self, endpoint):
        sessions = self.get_all_sessions_endpoint(endpoint)
        return [str(s) for s in sessions]

    def cleanup(self):
        self.session_manager.clean_up_all()

    def cleanup_endpoint(self, endpoint):
        for session in self.get_all_sessions_endpoint(endpoint):
            session.delete()

    def delete_session_by_name(self, name):
        self.session_manager.delete_client(name)

    def delete_session_by_id(self, endpoint, session_id):
        name = self.session_manager.get_session_name_by_id_endpoint(session_id, endpoint)

        if name in self.session_manager.get_sessions_list():
            self.delete_session_by_name(name)
        else:
            http_client = self._http_client(endpoint)
            response = http_client.get_session(session_id)
            http_client = self._http_client(endpoint)
            session = self._livy_session(http_client, {constants.LIVY_KIND_PARAM: response[constants.LIVY_KIND_PARAM]},
                                        self.ipython_display, session_id)
            session.delete()

    def add_session(self, name, endpoint, skip_if_exists, properties):
        if skip_if_exists and (name in self.session_manager.get_sessions_list()):
            self.logger.debug(u"Skipping {} because it already exists in list of sessions.".format(name))
            return
        http_client = self._http_client(endpoint)
        session = self._livy_session(http_client, properties, self.ipython_display)
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
            return self.session_manager.get_session(client_name)

    def get_managed_clients(self):
        return self.session_manager.sessions

    @staticmethod
    def _livy_session(http_client, properties, ipython_display,
                      session_id=-1):
        return LivySession(http_client, properties, ipython_display,
                           session_id, heartbeat_timeout=conf.livy_server_heartbeat_timeout_seconds())

    def _http_client(self, endpoint):
        if endpoint not in self._http_clients:
            self._http_clients[endpoint] = LivyReliableHttpClient.from_endpoint(endpoint)
        return self._http_clients[endpoint]
