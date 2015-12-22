from mock import MagicMock
from nose.tools import with_setup
from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.livyclientlib.pandaspysparklivyclient import PandasPysparkLivyClient
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException

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
    client = PandasPysparkLivyClient(mock_spark_session, 10)
    client.execute = execute_m = MagicMock()
    execute_responses = []


def _teardown():
    global execute_m
    execute_m.reset_mock()


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_pyspark_livy():
    # result from livy
    result_json = (True,
                   """['{\"buildingID\":0,\"date\":\"6/1/13\",\"temp_diff\":12}',
                   '{\"buildingID\":1,\"date\":\"6/1/13\",\"temp_diff\":0}']""")
    execute_m.return_value = result_json

    # desired pandas df
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0}]
    desired_df = pd.DataFrame(records)

    command = "command"
    df = client.execute_sql(command)

    execute_m.assert_called_with('sqlContext.sql("""{}""").toJSON().take({})'.format(command, 10))
    assert_frame_equal(desired_df, df)


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_pyspark_livy_no_results():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = "command"
    result_json = "[]"
    result_columns = "['buildingID', 'date', 'temp_diff']"
    execute_responses = [(True, result_json), (True, result_columns)]
    execute_m.side_effect = _next_response_execute

    # pandas to return
    columns = eval(result_columns)
    desired_df = pd.DataFrame.from_records(list(), columns=columns)

    df = client.execute_sql(command)

    # Verify basic calls were done
    execute_m.assert_called_with('sqlContext.sql("""{}""").columns'.format(command))
    assert_frame_equal(desired_df, df)

@with_setup(_setup, _teardown)
def test_execute_sql_pandas_pyspark_livy_bad_return():
    global execute_responses

    command = "command"
    result_json = (True, "something bad happened")
    execute_m.return_value = result_json

    try:
        client.execute_sql(command)
        assert False
    except DataFrameParseException:
        pass

@with_setup(_setup, _teardown)
def test_execute_sql_pandas_pyspark_livy_no_results_exception_in_columns():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = "command"
    result_json = "[]"
    some_exception = "some exception"
    execute_responses = [(True, result_json), (False, some_exception)]
    execute_m.side_effect = _next_response_execute

    try:
        result = client.execute_sql(command)
        assert False
    except DataFrameParseException as e:
        execute_m.assert_called_with('sqlContext.sql("""{}""").columns'.format(command))
        assert e.out == some_exception


@with_setup(_setup, _teardown)
def test_execute_sql_pandas_pyspark_livy_some_exception():
    # Set up spark session to return empty JSON and then columns
    command = "command"
    some_exception = "some awful exception"
    execute_m.return_value = (False, some_exception)

    try:
        result = client.execute_sql(command)
        assert False
    except DataFrameParseException as e:
        execute_m.assert_called_with('sqlContext.sql("""{}""").toJSON().take({})'.format(command, 10))
        assert e.out == some_exception
