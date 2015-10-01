# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json

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
        records_text = self.execute(self._make_sql_take(command))

        json_array = "[{}]".format(",".join(records_text.split("\n")))

        return pd.DataFrame(json.loads(json_array))

    def close_session(self):
        super(PandasScalaLivyClient, self).close_session()

    @property
    def language(self):
        return super(PandasScalaLivyClient, self).language

    def _make_sql_take(self, command):
        return 'sqlContext.sql("{}").toJSON.take({}).foreach(println)'.format(command, str(self._max_take_rows))
