# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class LivySessionState(object):
    def __init__(self, session_guid, session_id, connection_string, kind, sql_context_created, version="0.0.0"):
        self._session_guid = session_guid
        self._session_id = session_id
        self._kind = kind
        self._sql_context_created = sql_context_created
        self._version = version
        self._connection_string = connection_string

    @property
    def session_guid(self):
        return self._session_guid

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, value):
        self._session_id = value

    @property
    def kind(self):
        return self._kind

    @property
    def sql_context_created(self):
        return self._sql_context_created

    @sql_context_created.setter
    def sql_context_created(self, value):
        self._sql_context_created = value

    @property
    def version(self):
        return self._version

    @property
    def connection_string(self):
        return self._connection_string

    def to_dict(self):
        return {"GUID": self.session_guid, "id": self.session_id, "kind": self.kind, "sqlcontext": self.sql_context_created,
                "version": self.version, "connectionstring": self.connection_string}
