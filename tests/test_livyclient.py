from mock import MagicMock

from remotespark.livyclientlib.livyclient import LivyClient
from remotespark.livyclientlib.utils import get_connection_string
from remotespark.livyclientlib.livysessionstate import LivySessionState


def test_create_sql_context_automatically():
    mock_spark_session = MagicMock()
    LivyClient(mock_spark_session)

    mock_spark_session.create_sql_context.assert_called_with()


def test_execute_code():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    command = "command"

    client.execute(command)

    mock_spark_session.create_sql_context.assert_called_with()
    mock_spark_session.wait_for_status.assert_called_with("idle", 3600)
    mock_spark_session.execute.assert_called_with(command)


def test_execute_sql():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    command = "command"

    client.execute_sql(command)

    mock_spark_session.create_sql_context.assert_called_with()
    mock_spark_session.wait_for_status.assert_called_with("idle", 3600)
    mock_spark_session.execute.assert_called_with("sqlContext.sql(\"{}\").collect()".format(command))


def test_serialize():
    url = "url"
    username = "username"
    password = "password"
    connection_string = get_connection_string(url, username, password)
    http_client = MagicMock()
    http_client.connection_string = connection_string
    kind = "scala"
    session_id = "-1"
    sql_created = False
    session = MagicMock()
    session.get_state.return_value = LivySessionState(session_id, connection_string, kind, sql_created)

    client = LivyClient(session)

    serialized = client.serialize()

    assert serialized["connectionstring"] == connection_string
    assert serialized["id"] == "-1"
    assert serialized["language"] == kind
    assert serialized["sqlcontext"] == sql_created
    assert serialized["version"] == "0.0.0"
    assert len(serialized.keys()) == 5
