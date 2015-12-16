from mock import MagicMock
from nose.tools import with_setup
from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.livyclientlib.pandasscalalivyclient import PandasScalaLivyClient
from remotespark.livyclientlib.result import Result, DataFrameResult


mock_spark_session = None
client = None
execute_m = None
execute_responses = []


def _next_response_execute(*args):
    global execute_responses

    val = execute_responses[0]
    execute_responses = execute_responses[1:]
    return val


def _setup():
    global mock_spark_session, client, execute_m, execute_responses

    mock_spark_session = MagicMock()
    client = PandasScalaLivyClient(mock_spark_session, 10)
    client.execute = execute_m = MagicMock()
    execute_responses = []


def _teardown():
    global execute_m
    execute_m.reset_mock()


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_scala_livy():
    # result from livy
    result_json = "{\"buildingID\":0,\"date\":\"6/1/13\",\"temp_diff\":12}\n" \
                      "{\"buildingID\":1,\"date\":\"6/1/13\",\"temp_diff\":0}"
    execute_m.return_value = result_json

    # desired pandas df
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0}]
    desired_df = pd.DataFrame(records)

    command = "command"
    result = client.execute_sql(command)

    execute_m.assert_called_with('sqlContext.sql("""{}""").toJSON.take({}).foreach(println)'.format(command, 10))

    assert isinstance(result, Result)
    assert isinstance(result, DataFrameResult)
    mock_shell = MagicMock()
    df = result.render(mock_shell)
    assert not mock_shell.write.called
    assert not mock_shell.write_err.called
    assert_frame_equal(desired_df, df)


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_scala_livy_no_results():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = "command"
    result_json = ""
    result_columns = "res1: Array[String] = Array(buildingID, date, temp_diff)"
    execute_responses = [result_json, result_columns]
    execute_m.side_effect = _next_response_execute

    # pandas to return
    columns = ["buildingID", "date", "temp_diff"]
    desired_df = pd.DataFrame.from_records(list(), columns=columns)

    result = client.execute_sql(command)

    # Verify basic calls were done
    execute_m.assert_called_with('sqlContext.sql("""{}""").columns'.format(command))
    
    assert isinstance(result, Result)
    assert isinstance(result, DataFrameResult)
    mock_shell = MagicMock()
    df = result.render(mock_shell)
    assert not mock_shell.write.called
    assert not mock_shell.write_err.called
    assert_frame_equal(desired_df, df)


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_scala_livy_no_results_exception_in_columns():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = "command"
    result_json = ""
    some_exception = "some exception"
    execute_responses = [result_json, some_exception]
    execute_m.side_effect = _next_response_execute

    result = client.execute_sql(command)

    # Verify basic calls were done
    execute_m.assert_called_with('sqlContext.sql("""{}""").columns'.format(command))

    # Verify result is exception
    assert result == some_exception


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_scala_livy_some_exception():
    # Set up spark session to return empty JSON and then columns
    command = "command"
    some_exception = "some awful exception"
    execute_m.return_value = some_exception

    result = client.execute_sql(command)

    # Verify basic calls were done
    execute_m.assert_called_with('sqlContext.sql("""{}""").toJSON.take({}).foreach(println)'.format(command, 10))

    # Verify result is desired pandas dataframe
    assert some_exception == result
