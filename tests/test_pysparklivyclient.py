from mock import MagicMock
from nose.tools import with_setup, assert_equals
from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.utils.constants import LONG_RANDOM_VARIABLE_NAME
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException
from remotespark.livyclientlib.pysparklivyclient import PysparkLivyClient
from remotespark.livyclientlib.sqlquery import SQLQuery

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
    client = PysparkLivyClient(mock_spark_session)
    client.execute = execute_m = MagicMock()
    execute_responses = []


def _teardown():
    global execute_m
    execute_m.reset_mock()


@with_setup(_setup, _teardown)
def test_execute_sql_pyspark_livy():
    # result from livy
    result_json = (True,
                   """{"buildingID":0,"date":"6/1/13","temp_diff":12}
{"buildingID":1,"date":"6/1/13","temp_diff":0}""")
    execute_m.return_value = result_json

    # desired pandas df
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': u'0'}]
    desired_df = pd.DataFrame(records)
    desired_df["date"] = pd.to_datetime(desired_df["date"])
    desired_df["temp_diff"] = pd.to_numeric(desired_df["temp_diff"])

    command = SQLQuery("command", maxrows=50)
    df = client.execute_sql(command)

    execute_m.assert_called_with('for {} in sqlContext.sql("""{}""").toJSON().take({}): print({})' \
                                 .format(LONG_RANDOM_VARIABLE_NAME, command.query, 50,
                                         LONG_RANDOM_VARIABLE_NAME))
    assert_frame_equal(desired_df, df)


@with_setup(_setup, _teardown)
def test_execute_sql_pyspark_livy_no_results():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = SQLQuery("command")
    result_json = ""
    result_columns = "buildingID\ndate\ntemp_diff"
    execute_responses = [(True, result_json), (True, result_columns)]
    execute_m.side_effect = _next_response_execute

    # pandas to return
    columns = ['buildingID', 'date', 'temp_diff']
    desired_df = pd.DataFrame.from_records([], columns=columns)

    df = client.execute_sql(command)

    # Verify basic calls were done
    execute_m.assert_called_with('for {} in sqlContext.sql("""{}""").columns: print({})' \
                                 .format(LONG_RANDOM_VARIABLE_NAME, command.query,
                                         LONG_RANDOM_VARIABLE_NAME))
    assert_frame_equal(desired_df, df)


@with_setup(_setup, _teardown)
def test_execute_sql_pyspark_livy_bad_return():
    global execute_responses

    command = SQLQuery("command")
    result_json = (True, "something bad happened")
    execute_m.return_value = result_json

    try:
        client.execute_sql(command)
        assert False
    except DataFrameParseException:
        pass


@with_setup(_setup, _teardown)
def test_execute_sql_pyspark_livy_no_results_exception_in_columns():
    global execute_responses

    # Set up spark session to return empty JSON and then columns
    command = SQLQuery("command")
    result_json = ""
    some_exception = "some exception"
    execute_responses = [(True, result_json), (False, some_exception)]
    execute_m.side_effect = _next_response_execute

    try:
        client.execute_sql(command)
        assert False
    except DataFrameParseException as e:
        execute_m.assert_called_with('for {} in sqlContext.sql("""{}""").columns: print({})' \
                                     .format(LONG_RANDOM_VARIABLE_NAME, command.query,
                                             LONG_RANDOM_VARIABLE_NAME))
        assert e.out == some_exception


@with_setup(_setup, _teardown)
def test_execute_sql_pyspark_livy_some_exception():
    # Set up spark session to return empty JSON and then columns
    command = SQLQuery("command", maxrows=10)
    some_exception = "some awful exception"
    execute_m.return_value = (False, some_exception)

    try:
        client.execute_sql(command)
        assert False
    except DataFrameParseException as e:
        execute_m.assert_called_with('for {} in sqlContext.sql("""{}""").toJSON().take({}): print({})' \
                                     .format(LONG_RANDOM_VARIABLE_NAME, command.query, 10,
                                             LONG_RANDOM_VARIABLE_NAME))
        assert e.out == some_exception


@with_setup(_setup, _teardown)
def test_pyspark_livy_sql_options():
    query = "abc"

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").toJSON().collect(): print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").toJSON().sample(False, 0.25).collect(): print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").toJSON().sample(False, 0.33).take(3234): print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1, only_columns=True)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").columns: print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.999, maxrows=-1, only_columns=True)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").columns: print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.01, maxrows=3, only_columns=True)
    assert_equals(client._get_command_for_query(sqlquery),
                  'for {} in sqlContext.sql("""{}""").columns: print({})' \
                  .format(LONG_RANDOM_VARIABLE_NAME, query,
                          LONG_RANDOM_VARIABLE_NAME))


