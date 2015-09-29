# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd

from .log import Log
from .livyclient import LivyClient


class PandasPysparkLivyClient(LivyClient):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, session, max_take_rows):
        super(PandasPysparkLivyClient, self).__init__(session)
        self._max_take_rows = max_take_rows

    def execute(self, commands):
        return super(PandasPysparkLivyClient, self).execute(commands)

    def execute_sql(self, command):
        self._session.create_sql_context()

        records_text = self.execute(self._make_sql_take(command))
        columns_text = self.execute(self._make_sql_columns(command))

        records = eval(records_text)
        columns = eval(columns_text)

        return pd.DataFrame.from_records(records, columns=columns)

    def close_session(self):
        super(PandasPysparkLivyClient, self).close_session()

    def language(self):
        super(PandasPysparkLivyClient, self).language()

    def _make_sql_take(self, command):
        return 'sqlContext.sql("{}").take({})'.format(command, str(self._max_take_rows))

    @staticmethod
    def _make_sql_columns(command):
        return 'sqlContext.sql("{}").columns'.format(command)


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
