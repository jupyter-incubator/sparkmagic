# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json
import pandas as pd

import remotespark.utils.configuration as conf
from remotespark.utils.log import Log
from remotespark.utils.utils import coerce_pandas_df_to_numeric_datetime

from .dataframeparseexception import DataFrameParseException
from .sqlquery import SQLQuery


class LivyClient(object):
    """Spark client for Livy session"""

    def __init__(self, session):
        self.logger = Log("LivyClient")
        self._session = session
        self._execute_timeout_seconds = conf.execute_timeout_seconds()

    def __str__(self):
        return str(self._session)

    def start(self):
        self._session.create_sql_context()

    def serialize(self):
        return self._session.get_state().to_dict()

    def get_logs(self):
        try:
            return True, self._session.logs
        except ValueError as err:
            return False, "{}".format(err)

    def execute(self, commands):
        self._session.wait_for_idle(self._execute_timeout_seconds)
        return self._session.execute(commands)

    def execute_sql(self, sqlquery):
        return self._execute_sql_helper(sqlquery)

    def close_session(self):
        self._session.delete()

    @property
    def kind(self):
        return self._session.kind

    @property
    def session_id(self):
        return self._session.id

    @property
    def status(self):
        return self._session.status

    def _execute_sql_helper(self, sqlquery):
        (success, records_text) = self._get_records(sqlquery)
        if not success:
            raise DataFrameParseException(records_text)
        if records_text == "":
            # If there are no records, show some columns at least.
            records_text = self._get_columns(sqlquery)
            return self._columns_to_dataframe(records_text)
        else:
            return self._records_to_dataframe(records_text)

    def _get_records(self, sqlquery):
        command = self._get_command_for_query(sqlquery)
        return self.execute(command)

    def _get_columns(self, sqlquery):
        sqlquery2 = SQLQuery.get_only_columns_query(sqlquery)
        command = self._get_command_for_query(sqlquery2)
        (success, out) = self.execute(command)
        if success:
            return out
        else:
            raise DataFrameParseException(out)

    @staticmethod
    def _columns_to_dataframe(columns_text):
        return pd.DataFrame.from_records([], columns=columns_text.split('\n'))

    @staticmethod
    def _records_to_dataframe(records_text):
        strings = records_text.split('\n')
        try:
            df = pd.DataFrame([json.loads(s) for s in strings])
            coerce_pandas_df_to_numeric_datetime(df)
            return df
        except ValueError:
            raise DataFrameParseException("Cannot parse object as JSON: '{}'".format(strings))

    # Override this!
    def _get_command_for_query(self, sqlquery):
        raise NotImplementedError()
