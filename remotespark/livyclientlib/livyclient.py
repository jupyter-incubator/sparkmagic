# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .log import Log


class LivyClient(object):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, spark_session, pyspark_session):
        self._spark_session = spark_session
        self._pyspark_session = pyspark_session

    def execute_scala(self, commands):
        self._spark_session.wait_for_state("idle")
        return self._spark_session.execute(commands)

    def execute_scala_sql(self, command):
        self._spark_session.create_sql_context()
        return self.execute_scala('sqlContext.sql("' + command + '").collect()')

    def execute_pyspark(self, commands):
        self._pyspark_session.wait_for_state("idle")
        return self._pyspark_session.execute(commands)

    def execute_pyspark_sql(self, command):
        self._pyspark_session.create_sql_context()
        return self.execute_pyspark("sqlContext.sql('" + command + "').collect()")

    def close_sessions(self):
        self._spark_session.delete()
        self._pyspark_session.delete()
