from mock import MagicMock
from nose.tools import with_setup

from remotespark.livyclientlib.pandaslivyclientbase import PandasLivyClientBase
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException

import pandas as pd
from pandas.util.testing import assert_frame_equal

mock_spark_session = None
client = None


def _setup():
    global mock_spark_session, client

    mock_spark_session = MagicMock()
    mock_spark_session.execute = MagicMock(return_value=(True, ""))
    client = PandasLivyClientBase(mock_spark_session, 10)

def _teardown():
    pass

@with_setup(_setup, _teardown)
def test_execute_sql():
    records = (True, "records")
    no_records = False
    result_columns = pd.DataFrame([{'a': 1}])
    result_data = pd.DataFrame([{'b': 2}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_sql(records)
    assert_frame_equal(result, result_data)

@with_setup(_setup, _teardown)
def test_execute_sql_no_results():
    records = (True, "records")
    no_records = True
    result_columns = pd.DataFrame([{'z':26}])
    result_data = pd.DataFrame([{'y':25}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)
    client.make_context_columns = MagicMock()

    result = client.execute_sql(records)
    assert_frame_equal(result, result_columns)

@with_setup(_setup, _teardown)
def test_execute_sql_some_exception():
    records = (True, "records")
    no_records = False
    result_columns = pd.DataFrame([{'Ricky':22}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(side_effect=DataFrameParseException)

    try:
        result = client.execute_sql(records)
        assert False
    except DataFrameParseException as e:
        pass

@with_setup(_setup, _teardown)
def test_execute_hive():
    records = (True, "records")
    no_records = False
    result_columns = pd.DataFrame([{'g': -1}])
    result_data = pd.DataFrame([{'k': -2}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_hive(records)
    assert_frame_equal(result, result_data)

@with_setup(_setup, _teardown)
def test_execute_hive_no_results():
    records = (True, "records")
    no_records = True
    result_columns = pd.DataFrame([{'abc': 123}])
    result_data = pd.DataFrame([{'xyz': 890}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)
    client.make_context_columns = MagicMock()

    result = client.execute_hive(records)
    assert_frame_equal(result, result_columns)

@with_setup(_setup, _teardown)
def test_execute_hive_some_exception():
    records = (True, "records")
    no_records = False
    result_columns = pd.DataFrame([{'a': 0}])

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(side_effect=DataFrameParseException)

    try:
        result = client.execute_hive(records)
        assert False
    except DataFrameParseException as e:
        pass
