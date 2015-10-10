# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .log import Log


class LivyClient(object):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, session, execute_timeout_seconds=3600):
        self._session = session
        self._session.create_sql_context()
        self._execute_timeout_seconds = execute_timeout_seconds

    def execute(self, commands):
        self._session.wait_for_state("idle", self._execute_timeout_seconds)
        return self._session.execute(commands)

    def execute_sql(self, command):
        return self.execute('sqlContext.sql("{}").collect()'.format(command))

    def close_session(self):
        self._session.delete()

    @property
    def language(self):
        return self._session.language
