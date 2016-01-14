# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .pandaslivyclientbase import PandasLivyClientBase


class PandasRLivyClient(PandasLivyClientBase):
    """Spark client for Livy session in R"""

    def make_context_columns(self, context_name, command):
        raise NotImplementedError()

    def get_records(self, context_name, command, max_take_rows):
        raise NotImplementedError()
