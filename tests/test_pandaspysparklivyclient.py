from mock import MagicMock
from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.livyclientlib.pandaspysparklivyclient import PandasPysparkLivyClient


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

        mock_spark_session.wait_for_status.assert_called_with("idle", 3600)
        mock_spark_session.execute.assert_called_with(command)

    def test_execute_sql_pandas_pyspark_livy(self):
        mock_spark_session = MagicMock()
        client = PandasPysparkLivyClient(mock_spark_session, 10)

        # Set up spark session to return JSON
        result_json = "['{\"buildingID\":0,\"date\":\"6/1/13\",\"temp_diff\":12}','{\"buildingID\":1,\"date\":\"6/1/13\",\"temp_diff\":0}']"
        self.execute_responses = [result_json]
        mock_spark_session.execute.side_effect = self._next_response_execute

        # pandas to return
        records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12}, {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0}]
        desired_result = pd.DataFrame(records)

        command = "command"

        result = client.execute_sql(command)

        # Verify basic calls were done
        mock_spark_session.create_sql_context.assert_called_with()
        mock_spark_session.wait_for_status.assert_called_with("idle", 3600)

        # Verify result is desired pandas dataframe
        assert_frame_equal(desired_result, result)

    def test_execute_sql_pandas_pyspark_livy_no_results(self):
        mock_spark_session = MagicMock()
        client = PandasPysparkLivyClient(mock_spark_session, 10)

        # Set up spark session to return JSON
        result_json = "[]"
        result_columns = "['buildingID', 'date', 'temp_diff']"
        self.execute_responses = [result_json, result_columns]
        mock_spark_session.execute.side_effect = self._next_response_execute

        # pandas to return
        columns = eval(result_columns)
        desired_result = pd.DataFrame.from_records(list(), columns=columns)

        command = "command"

        result = client.execute_sql(command)

        # Verify basic calls were done
        mock_spark_session.create_sql_context.assert_called_with()
        mock_spark_session.wait_for_status.assert_called_with("idle", 3600)

        # Verify result is desired pandas dataframe
        assert_frame_equal(desired_result, result)