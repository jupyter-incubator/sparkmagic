# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import json
import re

from remotespark.utils.constants import Constants
from .pandaslivyclientbase import PandasLivyClientBase
from .dataframeparseexception import DataFrameParseException

class PandasPysparkLivyClient(PandasLivyClientBase):
    """Spark client for Livy session in PySpark"""

    def make_context_columns(self, context_name, command):
        return 'for {} in {}.sql("""{}""").columns: print({})'.format(Constants.long_random_variable_name,
                                                                      context_name, command,
                                                                      Constants.long_random_variable_name)


    def get_records(self, context_name, command, max_take_rows):
        command = 'for {} in {}.sql("""{}""").toJSON().take({}): print({})'.format(Constants.long_random_variable_name,
                                                                                   context_name, command, max_take_rows,
                                                                                   Constants.long_random_variable_name)
        return self.execute(command)


    def no_records(self, records_text):
        return records_text == ""
