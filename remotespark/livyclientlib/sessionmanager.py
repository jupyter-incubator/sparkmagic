# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.log import Log


class SessionManager(object):
    """Livy session manager"""

    def __init__(self):
        self.logger = Log("SessionManager")

        self._sessions = dict()

    @property
    def sessions(self):
        return self._sessions

    def get_sessions_list(self):
        return list(self._sessions.keys())

    def get_sessions_info(self):
        return ["Name: {}\t{}".format(k, str(self._sessions[k])) for k in list(self._sessions.keys())]

    def add_session(self, name, session):
        if name in self._sessions:
            raise ValueError("Session with name '{}' already exists. Please delete the session"
                             " first if you intend to replace it.".format(name))

        self._sessions[name] = session

    def get_any_session(self):
        number_of_sessions = len(self._sessions)
        if number_of_sessions == 1:
            key = self.get_sessions_list()[0]
            return self._sessions[key]
        elif number_of_sessions == 0:
            raise AssertionError("You need to have at least 1 client created to execute commands.")
        else:
            raise AssertionError("Please specify the client to use. Possible sessions are {}".format(
                self.get_sessions_list()))
        
    def get_session(self, name):
        if name in self._sessions:
            return self._sessions[name]
        raise ValueError("Could not find '{}' session in list of saved sessions. Possible sessions are {}".format(
            name, self.get_sessions_list()))

    def get_session_id_for_client(self, name):
        if name in self.get_sessions_list():
            return self._sessions[name].session_id
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
            raise ValueError("Could not find '{}' session in list of saved sessions. Possible sessions are {}"
                             .format(name, self.get_sessions_list()))
