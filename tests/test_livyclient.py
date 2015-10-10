from mock import MagicMock

from remotespark.livyclientlib.livyclient import LivyClient


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
    mock_spark_session.wait_for_state.assert_called_with("idle", 3600)
    mock_spark_session.execute.assert_called_with(command)


def test_execute_sql():
    mock_spark_session = MagicMock()
    client = LivyClient(mock_spark_session)
    command = "command"

    client.execute_sql(command)

    mock_spark_session.create_sql_context.assert_called_with()
    mock_spark_session.wait_for_state.assert_called_with("idle", 3600)
    mock_spark_session.execute.assert_called_with("sqlContext.sql(\"{}\").collect()".format(command))
