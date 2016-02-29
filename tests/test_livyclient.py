from mock import MagicMock, PropertyMock, call
from nose.tools import with_setup, assert_equals
import pandas as pd
from pandas.util.testing import assert_frame_equal

from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException

from remotespark.livyclientlib.livyclient import LivyClient
from remotespark.livyclientlib.livysessionstate import LivySessionState
from remotespark.livyclientlib.sqlquery import SQLQuery
from remotespark.utils.utils import get_connection_string
from remotespark.utils.constants import SESSION_KIND_SPARK


mock_spark_session = None
client = None


def _setup():
    global mock_spark_session, client

    mock_spark_session = MagicMock()
    mock_spark_session.execute = MagicMock(return_value=(True, ""))
    client = LivyClient(mock_spark_session)


def _teardown():
    pass


def test_doesnt_create_sql_context_automatically():
    mock_spark_session = MagicMock()
    LivyClient(mock_spark_session)
    assert not mock_spark_session.create_sql_context.called


def test_start_creates_sql_context():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    client.start()
    mock_spark_session.create_sql_context.assert_called_with()


def test_execute_code():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    client.start()
    command = "command"

    client.execute(command)

    mock_spark_session.create_sql_context.assert_called_with()
    mock_spark_session.wait_for_idle.assert_called_with(3600)
    mock_spark_session.execute.assert_called_with(command)


def test_serialize():
    url = "url"
    username = "username"
    password = "password"
    connection_string = get_connection_string(url, username, password)
    http_client = MagicMock()
    http_client.connection_string = connection_string
    kind = SESSION_KIND_SPARK
    session_id = "-1"
    sql_created = False
    session = MagicMock()
    session.get_state.return_value = LivySessionState(session_id, connection_string, kind, sql_created)

    client = LivyClient(session)
    client.start()

    serialized = client.serialize()

    assert serialized["connectionstring"] == connection_string
    assert serialized["id"] == "-1"
    assert serialized["kind"] == kind
    assert serialized["sqlcontext"] == sql_created
    assert serialized["version"] == "0.0.0"
    assert len(serialized.keys()) == 5


def test_close_session():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    client.start()

    client.close_session()

    mock_spark_session.delete.assert_called_once_with()


def test_kind():
    kind = "pyspark"
    mock_spark_session = MagicMock()
    language_mock = PropertyMock(return_value=kind)
    type(mock_spark_session).kind = language_mock
    client = LivyClient(mock_spark_session)
    client.start()

    l = client.kind

    assert l == kind


def test_session_id():
    session_id = "0"
    mock_spark_session = MagicMock()
    session_id_mock = PropertyMock(return_value=session_id)
    type(mock_spark_session).id = session_id_mock
    client = LivyClient(mock_spark_session)
    client.start()

    i = client.session_id

    assert i == session_id


def test_get_logs_returns_session_logs():
    logs = "hi"
    mock_spark_session = MagicMock()
    mock_spark_session.get_logs = MagicMock(return_value=logs)
    client = LivyClient(mock_spark_session)

    res, logs_r = client.get_logs()

    assert res
    assert logs_r == logs


def test_get_logs_returns_false_with_value_error():
    err = "err"
    mock_spark_session = MagicMock()
    mock_spark_session.get_logs = MagicMock(side_effect=ValueError(err))
    client = LivyClient(mock_spark_session)

    res, logs_r = client.get_logs()

    assert not res
    assert logs_r == err


@with_setup(_setup, _teardown)
def test_execute_sql():
    sqlquery = SQLQuery("HERE IS THE QUERY", "take", 100, 0.2)
    result = """{"z":100,"y":50}
{"z":25,"y":10}"""
    result_data = pd.DataFrame([{'z': 100, 'y': 50}, {'z':25, 'y':10}])
    mock_spark_session.kind = "pyspark"
    mock_spark_session.execute.return_value = (True, result)
    result = client.execute_sql(sqlquery)
    assert_frame_equal(result, result_data)
    mock_spark_session.execute.assert_called_once_with(sqlquery.to_command("pyspark"))


@with_setup(_setup, _teardown)
def test_execute_sql_no_results():
    sqlquery = SQLQuery("SHOW TABLES", "take", maxrows=-1)
    result1 = ""
    result2 = """column_a
THE_SECOND_COLUMN"""
    result_data = pd.DataFrame.from_records([], columns=['column_a', 'THE_SECOND_COLUMN'])
    def calls(c):
        if c == sqlquery.to_command("spark"):
            return True, result1
        else:
            return True, result2
    mock_spark_session.execute = MagicMock(wraps=calls)
    mock_spark_session.kind = "spark"
    result = client.execute_sql(sqlquery)
    assert_frame_equal(result, result_data)
    assert_equals(mock_spark_session.execute.mock_calls,
                  [call(sqlquery.to_command("spark")),
                   call(SQLQuery.as_only_columns_query(sqlquery).to_command("spark"))])


@with_setup(_setup, _teardown)
def test_execute_sql_some_exception():
    sqlquery = SQLQuery("HERE IS THE QUERY", "take", 100, 0.2)
    client.execute = MagicMock(return_value=(False, ''))
    mock_spark_session.kind = "pyspark"

    try:
        result = client.execute_sql(sqlquery)
        assert False
    except DataFrameParseException as e:
        pass

