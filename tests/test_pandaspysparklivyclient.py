from mock import MagicMock
from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.livyclientlib.pandaspysparklivyclient import PandasPysparkLivyClient
from remotespark.livyclientlib.pandaspysparklivyclient import Row


class TestPandasPysparkLivyClient:
    execute_responses = []

    def _next_response_execute(self, *args):
            val = self.execute_responses[0]
            self.execute_responses = self.execute_responses[1:]
            return val

    def test_execute_code_pandas_pyspark_livy(self):
        mock_spark_session = MagicMock()
        client = PandasPysparkLivyClient(mock_spark_session, 10)
        command = "command"

        client.execute(command)

        mock_spark_session.wait_for_state.assert_called_with("idle")
        mock_spark_session.execute.assert_called_with(command)

    def test_execute_sql_pandas_pyspark_livy(self):
        mock_spark_session = MagicMock()
        client = PandasPysparkLivyClient(mock_spark_session, 10)

        # Set up spark session to return JSON
        # result_json = '[{"id":0,"state":"starting","kind":"spark"},{"id":1,"state":"starting","kind":"spark"}]'
        result_collect = '[Row(0, "starting", "spark"), Row(1, "starting", "spark")]'
        result_columns = '["id", "state", "kind"]'
        self.execute_responses = [result_collect, result_columns]
        mock_spark_session.execute.side_effect = self._next_response_execute

        # pandas to return
        records = [Row(0, "starting", "spark"), Row(1, "starting", "spark")]
        columns = ["id", "state", "kind"]
        desired_result = pd.DataFrame.from_records(records, columns=columns)

        command = "command"

        result = client.execute_sql(command)

        # Verify basic calls were done
        mock_spark_session.create_sql_context.assert_called_with()
        mock_spark_session.wait_for_state.assert_called_with("idle")

        # Verify result is desired pandas dataframe
        assert_frame_equal(desired_result, result)