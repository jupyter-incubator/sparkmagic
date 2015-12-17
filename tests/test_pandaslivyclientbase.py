from mock import MagicMock
from nose.tools import with_setup

from remotespark.livyclientlib.pandaslivyclientbase import PandasLivyClientBase
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException

mock_spark_session = None
client = None


def _setup():
    global mock_spark_session, client

    mock_spark_session = MagicMock()
    client = PandasLivyClientBase(mock_spark_session, 10)


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_execute_sql():
    records = (True, "records")
    no_records = False
    result_columns = (True, "result_columns")
    result_data = (True, "result_data")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_sql(records)

    assert result == result_data


@with_setup(_setup, _teardown)
def test_execute_sql_no_results():
    records = (True, "records")
    no_records = True
    result_columns = (True, "result_columns")
    result_data = (True, "result_data")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_sql(records)

    assert result == result_columns


@with_setup(_setup, _teardown)
def test_execute_sql_some_exception():
    records = (True, "records")
    no_records = False
    result_columns = (True, "result_columns")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(side_effect=ValueError)

    try:
        result = client.execute_sql(records)
        assert False
    except DataFrameParseException as e:
        pass


@with_setup(_setup, _teardown)
def test_execute_hive():
    records = (True, "records")
    no_records = False
    result_columns = (True, "result_columns")
    result_data = (True, "result_data")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_hive(records)
    assert result == result_data


@with_setup(_setup, _teardown)
def test_execute_hive_no_results():
    records = (True, "records")
    no_records = True
    result_columns = (True, "result_columns")
    result_data = (True, "result_data")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(return_value=result_data)

    result = client.execute_hive(records)
    assert result == result_columns


@with_setup(_setup, _teardown)
def test_execute_hive_some_exception():
    records = (True, "records")
    no_records = False
    result_columns = (False, "result_columns")

    client.get_records = MagicMock(return_value=records)
    client.no_records = MagicMock(return_value=no_records)
    client.get_columns_dataframe = MagicMock(return_value=result_columns)
    client.get_data_dataframe = MagicMock(side_effect=ValueError)

    try:
        result = client.execute_hive(records)
        assert False
    except DataFrameParseException as e:
        pass
