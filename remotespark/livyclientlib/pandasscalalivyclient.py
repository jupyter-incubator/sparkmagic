# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json
import re

from .log import Log
from .livyclient import LivyClient


class PandasScalaLivyClient(LivyClient):
    """Spark client for Livy endpoint"""
    logger = Log()

    def __init__(self, session, max_take_rows):
        super(PandasScalaLivyClient, self).__init__(session)
        self._max_take_rows = max_take_rows

    def execute(self, commands):
        return super(PandasScalaLivyClient, self).execute(commands)

    def execute_sql(self, command):
        records_text = self.execute(self._make_sql_json_take(command))
        self.logger.debug("Records: " + records_text)

        if records_text == "":
            # If there are no records, show some columns at least.
            columns_text = self.execute(self._make_sql_columns(command))
            self.logger.debug("Columns: " + columns_text)

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
            json_array = "[{}]".format(",".join(records_text.split("\n")))
            return pd.DataFrame(json.loads(json_array))

    def close_session(self):
        super(PandasScalaLivyClient, self).close_session()

    @property
    def language(self):
        return super(PandasScalaLivyClient, self).language

    def _make_sql_json_take(self, command):
        return 'sqlContext.sql("{}").toJSON.take({}).foreach(println)'.format(command, str(self._max_take_rows))

    @staticmethod
    def _make_sql_columns(command):
        return 'sqlContext.sql("{}").columns'.format(command)
