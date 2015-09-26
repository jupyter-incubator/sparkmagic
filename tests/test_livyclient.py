from mock import MagicMock

from remotespark.livyclientlib.livyclient import LivyClient


def test_execute_scala():
    mock_sparksession = MagicMock()
    mock_pysparksession = MagicMock()
    client = LivyClient(mock_sparksession, mock_pysparksession)
    command = "command"

    client.execute_scala(command)

    mock_sparksession.wait_for_state.assert_called_with("idle")
    mock_sparksession.execute.assert_called_with(command)


def test_execute_pyspark():
    mock_sparksession = MagicMock()
    mock_pysparksession = MagicMock()
    client = LivyClient(mock_sparksession, mock_pysparksession)
    command = "command"

    client.execute_pyspark(command)

    mock_pysparksession.wait_for_state.assert_called_with("idle")
    mock_pysparksession.execute.assert_called_with(command)


def test_execute_scala_sql():
    mock_sparksession = MagicMock()
    mock_pysparksession = MagicMock()
    client = LivyClient(mock_sparksession, mock_pysparksession)
    command = "command"

    client.execute_scala_sql(command)

    mock_sparksession.create_sql_context.assert_called_with()
    mock_sparksession.wait_for_state.assert_called_with("idle")
    mock_sparksession.execute.assert_called_with("sqlContext.sql(\"{}\").collect()".format(command))


def test_execute_pyspark_sql():
    mock_sparksession = MagicMock()
    mock_pysparksession = MagicMock()
    client = LivyClient(mock_sparksession, mock_pysparksession)
    command = "command"

    client.execute_pyspark_sql(command)

    mock_pysparksession.create_sql_context.assert_called_with()
    mock_pysparksession.wait_for_state.assert_called_with("idle")
    mock_pysparksession.execute.assert_called_with("sqlContext.sql('{}').collect()".format(command))
