# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json
import re

from .dataframeparseexception import DataFrameParseException
from .pandaslivyclientbase import PandasLivyClientBase

class PandasScalaLivyClient(PandasLivyClientBase):
    """Spark client for Livy session in Scala"""
    GET_DATA_RE = re.compile('Array\[String\] = Array\((.*)\)')

    def __init__(self, session, max_take_rows):
        super(PandasScalaLivyClient, self).__init__(session, max_take_rows)

    def get_records(self, context_name, command, max_take_rows):
        command = '{}.sql("""{}""").toJSON.take({}).foreach(println)'.format(context_name, command, max_take_rows)
        return self.execute(command)

    def no_records(self, records_text):
        return records_text == ""

    def get_columns_dataframe(self, columns_text):
        # Columns will look something like this: 'res1: Array[String] = Array(tableName, isTemporary)'
        # We need to transform them into a list of strings: ["tableName", "isTemporary"]
        m = re.search(self.GET_DATA_RE, columns_text)

        # If we failed to find the columns
        if m is None:
            raise DataFrameParseException("Could not find columns in '{}'.".format(columns_text))

        captured_group = m.group(1)

        # If there are no columns
        if captured_group.strip() == "":
            raise DataFrameParseException("No data available in '{}'".format(columns_text))

        # Convert the columns into an array of text
        columns = [s.strip() for s in captured_group.split(",")]

        return pd.DataFrame.from_records([], columns=columns)

    def get_data_dataframe(self, records_text):
        json_array = records_text.split("\n")
        try:
            return pd.DataFrame([json.loads(s) for s in json_array])
        except ValueError:
            raise DataFrameParseException("Could not parse the object as JSON: '{}'".format(records_text))
