# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class MagicsSessionState(object):
    def __init__(self, session_id, connection_string, language, sql_context_created, version="0.0.0"):
        self._session_id = session_id
        self._language = language
        self._sql_context_created = sql_context_created
        self._version = version
        self._connection_string = connection_string

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, value):
        self._session_id = value

    @property
    def language(self):
        return self._language

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
        return {"id": self.session_id, "language": self.language, "sqlcontext": self.sql_context_created,
                "version": self.version, "connectionstring": self.connection_string}
