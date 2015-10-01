# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json

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
        records_text = self.execute(self._make_sql_json_take(command))
        self.logger.debug("Records: " + records_text)

        if records_text == "[]":
            # If there are no records, show some columns at least.
            columns_text = self.execute(self._make_sql_columns(command))
            self.logger.debug("Columns: " + columns_text)

            records = list()
            columns = eval(columns_text)

            return pd.DataFrame.from_records(records, columns=columns)
        else:
            jsonData = eval(records_text)
            jsonArray = "[{}]".format(",".join(jsonData))

            return pd.DataFrame(json.loads(jsonArray))

    def close_session(self):
        super(PandasPysparkLivyClient, self).close_session()

    @property
    def language(self):
        return super(PandasPysparkLivyClient, self).language

    def _make_sql_json_take(self, command):
        return 'sqlContext.sql("{}").toJSON().take({})'.format(command, str(self._max_take_rows))

    @staticmethod
    def _make_sql_columns(command):
        return 'sqlContext.sql("{}").columns'.format(command)
