# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json

from .pandaslivyclientbase import PandasLivyClientBase
from .result import DataFrameResult

class PandasPysparkLivyClient(PandasLivyClientBase):
    """Spark client for Livy endpoint in PySpark"""

    def __init__(self, session, max_take_rows):
        super(PandasPysparkLivyClient, self).__init__(session, max_take_rows)

    def get_records(self, context_name, command, max_take_rows):
        command = '{}.sql("{}").toJSON().take({})'.format(context_name, command, max_take_rows)
        return str(self.execute(command))

    def no_records(self, records_text):
        return records_text == "[]"

    def get_columns_dataframe(self, columns_text):
        records = list()
        columns = eval(columns_text)

        return DataFrameResult(pd.DataFrame.from_records(records,
                                                         columns=columns))

    def get_data_dataframe(self, records_text):
        json_data = eval(records_text)
        json_array = "[{}]".format(",".join(json_data))
        return DataFrameResult(pd.DataFrame(json.loads(json_array)))
