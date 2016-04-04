from mock import MagicMock, call
from nose.tools import with_setup, assert_equals, assert_false, raises
import pandas as pd
from pandas.util.testing import assert_frame_equal


from remotespark.utils.constants import LONG_RANDOM_VARIABLE_NAME
from remotespark.livyclientlib.sqlquery import SQLQuery
from remotespark.livyclientlib.command import Command
from remotespark.livyclientlib.exceptions import BadUserDataException
import remotespark.utils.configuration as conf


def _teardown():
    conf.load()


def test_sqlquery_initializes():
    query = "HERE IS MY SQL QUERY SELECT * FROM CREATE DROP TABLE"
    samplemethod = "take"
    maxrows = 120
    samplefraction = 0.6
    sqlquery = SQLQuery(query, samplemethod, maxrows, samplefraction)
    assert_equals(sqlquery.query, query)
    assert_equals(sqlquery.samplemethod, samplemethod)
    assert_equals(sqlquery.maxrows, maxrows)
    assert_equals(sqlquery.samplefraction, samplefraction)


@with_setup(teardown=_teardown)
def test_sqlquery_loads_defaults():
    defaults = {
        conf.default_samplemethod.__name__: "sample",
        conf.default_maxrows.__name__: 419,
        conf.default_samplefraction.__name__: 0.99,
    }
    conf.override_all(defaults)
    query = "DROP TABLE USERS;"
    sqlquery = SQLQuery(query)
    assert_equals(sqlquery.query, query)
    assert_equals(sqlquery.samplemethod, defaults[conf.default_samplemethod.__name__])
    assert_equals(sqlquery.maxrows, defaults[conf.default_maxrows.__name__])
    assert_equals(sqlquery.samplefraction, defaults[conf.default_samplefraction.__name__])


@raises(BadUserDataException)
def test_sqlquery_rejects_bad_data():
    query = "HERE IS MY SQL QUERY SELECT * FROM CREATE DROP TABLE"
    samplemethod = "foo"
    _ = SQLQuery(query, samplemethod)


def test_pyspark_livy_sql_options():
    query = "abc"

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=120)
    assert_equals(sqlquery._pyspark_command(),
                  Command('for {} in sqlContext.sql("""{}""").toJSON().take(120): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
    assert_equals(sqlquery._pyspark_command(),
                  Command('for {} in sqlContext.sql("""{}""").toJSON().collect(): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sqlquery._pyspark_command(),
                  Command('for {} in sqlContext.sql("""{}""").toJSON().sample(False, 0.25).collect(): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sqlquery._pyspark_command(),
                  Command('for {} in sqlContext.sql("""{}""").toJSON().sample(False, 0.33).take(3234): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))


def test_scala_livy_sql_options():
    query = "abc"

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=100)
    assert_equals(sqlquery._scala_command(),
                  Command('sqlContext.sql("""{}""").toJSON.take(100).foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
    assert_equals(sqlquery._scala_command(),
                  Command('sqlContext.sql("""{}""").toJSON.collect.foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sqlquery._scala_command(),
                  Command('sqlContext.sql("""{}""").toJSON.sample(false, 0.25).collect.foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sqlquery._scala_command(),
                  Command('sqlContext.sql("""{}""").toJSON.sample(false, 0.33).take(3234).foreach(println)'.format(query)))


def test_execute_sql():
    spark_events = MagicMock()
    sqlquery = SQLQuery("HERE IS THE QUERY", "take", 100, 0.2, spark_events=spark_events)
    sqlquery.to_command = MagicMock(return_value=MagicMock())
    result = """{"z":100,"y":50}
{"z":25,"y":10}"""
    sqlquery.to_command.return_value.execute = MagicMock(return_value=(True, result))
    result_data = pd.DataFrame([{'z': 100, 'y': 50}, {'z':25, 'y':10}])
    session = MagicMock()
    session.kind = "pyspark"
    result = sqlquery.execute(session)
    assert_frame_equal(result, result_data)
    sqlquery.to_command.return_value.execute.assert_called_once_with(session)
    spark_events.emit_sql_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                         session.id, sqlquery.guid)
    spark_events.emit_sql_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                       session.id, sqlquery.guid,
                                                                       sqlquery.to_command.return_value.guid,
                                                                       True, "", "")


def test_execute_sql_no_results():
    global executed_once
    executed_once = False
    spark_events = MagicMock()
    sqlquery = SQLQuery("SHOW TABLES", "take", maxrows=-1, spark_events=spark_events)
    sqlquery.to_command = MagicMock()
    sqlquery.to_only_columns_query = MagicMock()
    result1 = ""
    result_data = pd.DataFrame([])
    session = MagicMock()
    sqlquery.to_command.return_value.execute.return_value = (True, result1)
    session.kind = "spark"
    result = sqlquery.execute(session)
    assert_frame_equal(result, result_data)
    sqlquery.to_command.return_value.execute.assert_called_once_with(session)
    spark_events.emit_sql_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                         session.id, sqlquery.guid,
                                                                         sqlquery.samplemethod, sqlquery.maxrows,
                                                                         sqlquery.samplefraction)
    spark_events.emit_sql_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                       session.id, sqlquery.guid,
                                                                       sqlquery.to_command.return_value.guid,
                                                                       True, "", "")


def test_execute_sql_failure_emits_event():
    spark_events = MagicMock()
    sqlquery = SQLQuery("HERE IS THE QUERY", "take", 100, 0.2, spark_events)
    sqlquery.to_command = MagicMock()
    sqlquery.to_command.return_value.execute = MagicMock(side_effect=ValueError('yo'))
    session = MagicMock()
    session.kind = "pyspark"
    try:
        result = sqlquery.execute(session)
        assert False
    except ValueError:
        sqlquery.to_command.return_value.execute.assert_called_once_with(session)
        spark_events.emit_sql_execution_start_event._assert_called_once_with(session.guid, session.kind,
                                                                             session.id, sqlquery.guid)
        spark_events.emit_sql_execution_end_event._assert_called_once_with(session.guid, session.kind,
                                                                           session.id, sqlquery.guid,
                                                                           sqlquery.to_command.return_value.guid,
                                                                           True, "ValueError", "yo")




