# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json
import re

from .livyclient import LivyClient


class PandasScalaLivyClient(LivyClient):
    """Spark client for Livy endpoint"""
    def __init__(self, session, max_take_rows):
        super(PandasScalaLivyClient, self).__init__(session)
        self._max_take_rows = max_take_rows

    def execute_sql(self, command):
        return self._execute_dataframe_helper("sqlContext", command)

    def execute_hive(self, command):
        return self._execute_dataframe_helper("hiveContext", command)

    def _execute_dataframe_helper(self, context_name, command):
        records_text = self.execute(self._make_context_json_take(context_name, command))

        if records_text == "":
            # If there are no records, show some columns at least.
            columns_text = self.execute(self._make_context_columns(context_name, command))

            records = list()

            # Columns will look something like this: 'res1: Array[String] = Array(tableName, isTemporary)'
            # We need to transform them into a list of strings: ["tableName", "isTemporary"]
            m = re.search('Array\[String\] = Array\((.*)\)', columns_text)

            # If we failed to find the columns
            if m is None:
                raise SyntaxError("Could not find columns in '{}'.".format(columns_text))

            captured_group = m.group(1)

            # If there are no columns
            if captured_group.strip() == "":
                return "No data available."

            # Convert the columns into an array of text
            columns = [s.strip() for s in captured_group.split(",")]

            return pd.DataFrame.from_records(records, columns=columns)
        else:
            try:
                json_array = "[{}]".format(",".join(records_text.split("\n")))
                return pd.DataFrame(json.loads(json_array))
            except ValueError:
                self.logger.error("Could not parse json array for sql.")
                return records_text

    def _make_context_json_take(self, context_name, command):
        return '{}.sql("{}").toJSON.take({}).foreach(println)'.format(context_name, command, str(self._max_take_rows))

    @staticmethod
    def _make_context_columns(context_name, command):
        return '{}.sql("{}").columns'.format(context_name, command)
