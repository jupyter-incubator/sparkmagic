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


def test_execute_sql():
    mock_sparksession = MagicMock()
    mock_pysparksession = MagicMock()
    client = LivyClient(mock_sparksession, mock_pysparksession)
    command = "command"

    client.execute_sql(command)

    mock_sparksession.wait_for_state.assert_called_with("idle")
    mock_sparksession.execute.assert_called_with("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\nimport "
                                                 "sqlContext.implicits._\nsqlContext.sql(\"{}\")"
                                                 ".collect()".format(command))
