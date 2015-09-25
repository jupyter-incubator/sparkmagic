# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .log import Log


class LivyClient(object):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, spark_session, pyspark_session):
        self._spark_session = spark_session
        self._pyspark_session = pyspark_session
        self._executed_sql = False

    def execute_scala(self, commands):
        self._spark_session.wait_for_state("idle")
        return self._spark_session.execute(commands)

    def execute_pyspark(self, commands):
        self._pyspark_session.wait_for_state("idle")
        return self._pyspark_session.execute(commands)

    def execute_sql(self, command):
        if not self._executed_sql:
            self.execute_pyspark(self._create_sql_context())
            self._executed_sql = True

        records_text = self.execute_pyspark(self._make_sql_collect(command))
        columns_text = self.execute_pyspark(self._make_sql_columns(command))

        records = eval(records_text)
        columns = eval(columns_text)

        import pandas as pd
        return pd.DataFrame.from_records(records, columns=columns)

    def close_sessions(self):
        self._spark_session.delete()
        self._pyspark_session.delete()

    def _create_sql_context(self):
        # TODO(aggftw): Create sqlContext by default on session start
        sql = "from pyspark.sql import SQLContext\nfrom pyspark.sql.types import *\nsqlContext = SQLContext(sc)"
        return sql

    def _make_sql_collect(self, command):
        sql = 'sqlContext.sql("' + command + '").collect()'
        return sql

    def _make_sql_columns(self, command):
        sql = 'sqlContext.sql("' + command + '").columns'
        return sql


class Row(tuple):
    def __new__(cls, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if args:
            # create row class or objects
            return tuple.__new__(cls, args)

        elif kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(cls, [kwargs[n] for n in names])
            row.__fields__ = names
            return row

        else:
            raise ValueError("No args or kwargs")
