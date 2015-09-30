# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .log import Log


class LivyClient(object):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, session):
        self._session = session

    def execute(self, commands):
        self._session.wait_for_state("idle")
        return self._session.execute(commands)

    def execute_sql(self, command):
        self._session.create_sql_context()
        return self.execute('sqlContext.sql("' + command + '").collect()')

    def close_session(self):
        self._session.delete()

    def language(self):
        return self._session.language
