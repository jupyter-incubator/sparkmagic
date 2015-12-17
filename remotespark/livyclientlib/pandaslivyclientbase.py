# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .livyclient import LivyClient
from .dataframeparseexception import DataFrameParseException

class PandasLivyClientBase(LivyClient):
    """Spark client for Livy endpoint that produces pandas df for sql and hive commands."""
    def __init__(self, session, max_take_rows):
        super(PandasLivyClientBase, self).__init__(session)
        self.max_take_rows = max_take_rows

    def execute_sql(self, command):
        return self._execute_dataframe_helper("sqlContext", command)

    def execute_hive(self, command):
        return self._execute_dataframe_helper("hiveContext", command)

    def _execute_dataframe_helper(self, context_name, command):
        (success, records_text) = self.get_records(context_name, command,
                                                   str(self.max_take_rows))
        if not success:
            raise DataFrameParseException(records_text)
        try:
            if self.no_records(records_text):
                # If there are no records, show some columns at least.
                records_text = self.get_columns(context_name, command)
                return self.get_columns_dataframe(records_text)
            else:
                return self.get_data_dataframe(records_text)
        except (ValueError, SyntaxError) as e:
            self.logger.error("Could not convert sql results to pandas DF.")
            raise DataFrameParseException(records_text)
            
    def get_columns(self, context_name, command):
        (success, out) = self.execute(self.make_context_columns(context_name,
                                                                command))
        if success:
            return out
        else:
            raise DataFrameParseException(out)

    @staticmethod
    def make_context_columns(context_name, command):
        return '{}.sql("""{}""").columns'.format(context_name, command)

    # Please override here down
    def get_records(self, context_name, command, max_take_rows):
        raise NotImplementedError()

    def no_records(self, records_text):
        raise NotImplementedError()

    def get_columns_dataframe(self, columns_text):
        raise NotImplementedError()

    def get_data_dataframe(self, records_text):
        raise NotImplementedError()
