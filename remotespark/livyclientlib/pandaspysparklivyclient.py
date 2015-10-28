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
        records_text = self.execute(self._make_sql_json_take(command))

        if records_text == "[]":
            # If there are no records, show some columns at least.
            columns_text = self.execute(self._make_sql_columns(command))

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

    def _make_sql_json_take(self, command):
        return 'sqlContext.sql("{}").toJSON().take({})'.format(command, str(self._max_take_rows))

    @staticmethod
    def _make_sql_columns(command):
        return 'sqlContext.sql("{}").columns'.format(command)
