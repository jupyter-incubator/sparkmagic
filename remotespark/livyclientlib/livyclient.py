# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .log import Log
from .configuration import get_configuration
from .constants import Constants


class LivyClient(object):
    """Spark client for Livy session"""

    def __init__(self, session):
        self.logger = Log("LivyClient")

        execute_timeout_seconds = get_configuration(Constants.execute_timeout_seconds, 3600)

        self._session = session
        self._session.create_sql_context()
        self._execute_timeout_seconds = execute_timeout_seconds

    def serialize(self):
        return self._session.get_state().to_dict()

    def execute(self, commands):
        self._session.wait_for_status("idle", self._execute_timeout_seconds)
        return self._session.execute(commands)

    def execute_sql(self, command):
        return self.execute('sqlContext.sql("{}").collect()'.format(command))

    def execute_hive(self, command):
        return self.execute('hiveContext.sql("{}").collect()'.format(command))

    def close_session(self):
        self._session.delete()

    @property
    def language(self):
        return self._session.language

    @property
    def session_id(self):
        return self._session.id
