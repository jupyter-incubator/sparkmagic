# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json

from .livyclient import LivyClient


class PandasPysparkLivyClient(LivyClient):
    """Spark client for Livy endpoint"""

    def __init__(self, session, max_take_rows):
        super(PandasPysparkLivyClient, self).__init__(session)
        self._max_take_rows = max_take_rows

    def execute_sql(self, command):
        return self._execute_dataframe_helper("sqlContext", command)

    def execute_hive(self, command):
        return self._execute_dataframe_helper("hiveContext", command)

    def _execute_dataframe_helper(self, context_name, command):
        records_text = self.execute(self._make_context_json_take(context_name, command))

        if records_text == "[]":
            # If there are no records, show some columns at least.
            columns_text = self.execute(self._make_context_columns(context_name, command))

            records = list()
            columns = eval(columns_text)

            return pd.DataFrame.from_records(records, columns=columns)
        else:
            try:
                json_data = eval(records_text)
                json_array = "[{}]".format(",".join(json_data))
                return pd.DataFrame(json.loads(json_array))
            except (ValueError, SyntaxError):
                self.logger.error("Could not parse json array for sql.")
                return records_text

    def _make_context_json_take(self, context_name, command):
        return '{}.sql("{}").toJSON().take({})'.format(context_name, command, str(self._max_take_rows))

    @staticmethod
    def _make_context_columns(context_name, command):
        return '{}.sql("{}").columns'.format(context_name, command)
