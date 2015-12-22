# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json
import re

from .pandaslivyclientbase import PandasLivyClientBase
from .dataframeparseexception import DataFrameParseException

class PandasPysparkLivyClient(PandasLivyClientBase):
    """Spark client for Livy session in PySpark"""
    GET_DATA_RE = re.compile("^\[\s*(('.*[^\\\\]',\s*)\s*('.*[^\\\\]')?)\s*\]$")
    PARSE_FIELDS_RE = re.compile("'(.*?[^\\\\])'")

    def __init__(self, session, max_take_rows):
        super(PandasPysparkLivyClient, self).__init__(session, max_take_rows)

    def get_records(self, context_name, command, max_take_rows):
        command = '{}.sql("""{}""").toJSON().take({})'.format(context_name, command, max_take_rows)
        return self.execute(command)

    def no_records(self, records_text):
        return records_text == "[]"

    def get_columns_dataframe(self, columns_text):
        return pd.DataFrame.from_records([], columns=self._extract_strings_from_array(columns_text))


    def get_data_dataframe(self, records_text):
        strings = self._extract_strings_from_array(records_text)
        try:
            return pd.DataFrame([json.loads(s) for s in strings])
        except ValueError:
            raise DataFrameParseException("Cannot parse object as JSON: '{}'".format(strings))


    def _extract_strings_from_array(self, s):
        match = re.match(self.GET_DATA_RE, s.strip().replace('\n', ''))
        if match is not None:
            inside_brackets = match.group(1)
            columns = list(re.findall(self.PARSE_FIELDS_RE, inside_brackets))
            return [string.replace("\\'", "'") for string in columns]
        else:
            raise DataFrameParseException("Cannot parse dataframe data in text '{}'".format(s))
