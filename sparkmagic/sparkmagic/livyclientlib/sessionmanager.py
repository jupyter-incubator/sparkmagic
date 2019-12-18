# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import atexit
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.livyclientlib.exceptions import SessionManagementException
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
import sparkmagic.utils.configuration as conf


class SessionManager(object):
    def __init__(self, ipython_display):
        self.logger = SparkLog(u"SessionManager")
        self.ipython_display = ipython_display

        self._sessions = dict()

        self._register_cleanup_on_exit()

    @property
    def sessions(self):
        return self._sessions

    def get_sessions_list(self):
        return list(self._sessions.keys())

    def get_sessions_info(self):
        return [u"Name: {}\t{}".format(k, str(self._sessions[k])) for k in list(self._sessions.keys())]

    def add_session(self, name, session):
        if name in self._sessions:
            raise SessionManagementException(u"Session with name '{}' already exists. Please delete the session"
                                             u" first if you intend to replace it.".format(name))

        self._sessions[name] = session

    def get_any_session(self):
        number_of_sessions = len(self._sessions)
        if number_of_sessions == 1:
            key = self.get_sessions_list()[0]
            return self._sessions[key]
        elif number_of_sessions == 0:
            raise SessionManagementException(u"You need to have at least 1 client created to execute commands.")
        else:
            raise SessionManagementException(u"Please specify the client to use. Possible sessions are {}".format(
                self.get_sessions_list()))
        
    def get_session(self, name):
        if name in self._sessions:
            return self._sessions[name]
        raise SessionManagementException(u"Could not find '{}' session in list of saved sessions. Possible sessions are {}".format(
            name, self.get_sessions_list()))

    def get_session_id_for_client(self, name):
        if name in self.get_sessions_list():
            return self._sessions[name].id
        return None

    def get_session_name_by_id_endpoint(self, id, endpoint):
        for (name, session) in self._sessions.items():
            if session.id == int(id) and session.endpoint == endpoint:
                return name
        return None

    def delete_client(self, name):
        self._remove_session(name)
    
    def clean_up_all(self):
        for name in self.get_sessions_list():
            self._remove_session(name)

    def _remove_session(self, name):
        if name in self.get_sessions_list():
            self._sessions[name].delete()
            del self._sessions[name]
        else:
            raise SessionManagementException(u"Could not find '{}' session in list of saved sessions. Possible sessions are {}"
                                             .format(name, self.get_sessions_list()))

    def _register_cleanup_on_exit(self):
        """
        Stop the livy sessions before python process exits for any reason (if enabled in conf)
        """
        if conf.cleanup_all_sessions_on_exit():
            def cleanup_spark_sessions():
                try:
                    self.clean_up_all()
                except Exception as e:
                    self.logger.error(u"Error cleaning up sessions on exit: {}".format(e))
                    pass
            atexit.register(cleanup_spark_sessions)
            self.ipython_display.writeln(u"Cleaning up livy sessions on exit is enabled")
