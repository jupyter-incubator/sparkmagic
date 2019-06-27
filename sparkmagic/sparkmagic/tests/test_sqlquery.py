# coding=utf-8
from mock import MagicMock, call
from nose.tools import with_setup, assert_equals, assert_false, raises
import pandas as pd
from pandas.util.testing import assert_frame_equal

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LONG_RANDOM_VARIABLE_NAME
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException


def _setup():
    pass
    

def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_to_command_pyspark():
    variable_name = "var_name"
    sqlquery = SQLQuery("Query")
    sqlquery._pyspark_command = MagicMock(return_value=MagicMock())
    sqlquery.to_command("pyspark", variable_name)
    sqlquery._pyspark_command.assert_called_with(variable_name)


@with_setup(_setup, _teardown)
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


@with_setup(_setup, _teardown)
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


@with_setup(_setup, _teardown)
@raises(BadUserDataException)
def test_sqlquery_rejects_bad_data():
    query = "HERE IS MY SQL QUERY SELECT * FROM CREATE DROP TABLE"
    samplemethod = "foo"
    _ = SQLQuery(query, samplemethod)


@with_setup(_setup, _teardown)
def test_pyspark_livy_sql_options():
    query = "abc"

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=120)
    assert_equals(sqlquery._pyspark_command("sqlContext"),
                  Command(u'import sys\nfor {} in sqlContext.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).take(120): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
    assert_equals(sqlquery._pyspark_command("sqlContext"),
                  Command(u'import sys\nfor {} in sqlContext.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).collect(): print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sqlquery._pyspark_command("sqlContext"),
                  Command(u'import sys\nfor {} in sqlContext.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.25).collect(): '
                          u'print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sqlquery._pyspark_command("sqlContext"),
                  Command(u'import sys\nfor {} in sqlContext.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.33).take(3234): '
                          u'print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query,
                                  LONG_RANDOM_VARIABLE_NAME)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sqlquery._pyspark_command("spark"),
                  Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.33).take(3234): '
                          u'print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, query, LONG_RANDOM_VARIABLE_NAME)))

@with_setup(_setup, _teardown)
def test_scala_livy_sql_options():
    query = "abc"

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=100)
    assert_equals(sqlquery._scala_command("sqlContext"),
                  Command('sqlContext.sql("""{}""").toJSON.take(100).foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
    assert_equals(sqlquery._scala_command("sqlContext"),
                  Command('sqlContext.sql("""{}""").toJSON.collect.foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sqlquery._scala_command("sqlContext"),
                  Command('sqlContext.sql("""{}""").toJSON.sample(false, 0.25).collect.foreach(println)'.format(query)))

    sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sqlquery._scala_command("sqlContext"),
                  Command('sqlContext.sql("""{}""").toJSON.sample(false, 0.33).take(3234).foreach(println)'.format(query)))

@with_setup(_setup, _teardown)
def test_r_livy_sql_options_spark():
        query = "abc"
        sqlquery = SQLQuery(query, samplemethod='take', maxrows=100)
        sqlContext = "sqlContext"

        assert_equals(sqlquery._r_command(sqlContext),
                      Command('for ({} in (jsonlite:::toJSON(take(sql({}, "{}"),100)))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, sqlContext, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
        assert_equals(sqlquery._r_command(sqlContext),
                      Command('for ({} in (jsonlite:::toJSON(collect(sql({}, "{}"))))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, sqlContext, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sqlquery._r_command(sqlContext),
                      Command('for ({} in (jsonlite:::toJSON(collect(sample(sql({}, "{}"), FALSE, 0.25))))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, sqlContext, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sqlquery._r_command(sqlContext),
                      Command('for ({} in (jsonlite:::toJSON(take(sample(sql({}, "{}"), FALSE, 0.33),3234)))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, sqlContext, query, LONG_RANDOM_VARIABLE_NAME)))


@with_setup(_setup, _teardown)
def test_execute_sql():
    spark_events = MagicMock()
    sqlquery = SQLQuery("HERE IS THE QUERY", "take", 100, 0.2, spark_events=spark_events)
    sqlquery.to_command = MagicMock(return_value=MagicMock())
    result = """{"z":100, "nullv":null, "y":50}
{"z":25, "nullv":null, "y":10}"""
    sqlquery.to_command.return_value.execute = MagicMock(return_value=(True, result))
    result_data = pd.DataFrame([{'z': 100, "nullv": None, 'y': 50}, {'z':25, "nullv":None, 'y':10}], columns=['z', "nullv", 'y'])
    session = MagicMock()
    session.kind = "pyspark"
    result = sqlquery.execute(session)
    assert_frame_equal(result, result_data)
    sqlquery.to_command.return_value.execute.assert_called_once_with(session)
    spark_events.emit_sql_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                         session.id, sqlquery.guid,
                                                                        'take', 100, 0.2)
    spark_events.emit_sql_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                       session.id, sqlquery.guid,
                                                                       sqlquery.to_command.return_value.guid,
                                                                       True, '','')


@with_setup(_setup, _teardown)
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
    spark_events.emit_sql_execution_start_event.assert_called_once_with(session.guid, session.kind,
                                                                         session.id, sqlquery.guid,
                                                                         sqlquery.samplemethod, sqlquery.maxrows,
                                                                         sqlquery.samplefraction)
    spark_events.emit_sql_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                       session.id, sqlquery.guid,
                                                                       sqlquery.to_command.return_value.guid,
                                                                       True, "", "")


@with_setup(_setup, _teardown)
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
        spark_events.emit_sql_execution_end_event.assert_called_once_with(session.guid, session.kind,
                                                                           session.id, sqlquery.guid,
                                                                           sqlquery.to_command.return_value.guid,
                                                                           False, 'ValueError', 'yo')


@with_setup(_setup, _teardown)
def test_unicode_sql():
    query = u"SELECT 'è'"
    longvar = LONG_RANDOM_VARIABLE_NAME

    sqlquery = SQLQuery(query, samplemethod='take', maxrows=120)
    assert_equals(sqlquery._pyspark_command("spark"),
                  Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).take(120): print({})'\
                          .format(longvar, query,
                                  longvar)))
    assert_equals(sqlquery._scala_command("spark"),
                  Command(u'spark.sql("""{}""").toJSON.take(120).foreach(println)'.format(query)))
    assert_equals(sqlquery._r_command("spark"),
                  Command(u'for ({} in (jsonlite:::toJSON(take(sql("{}"),120)))) {{cat({})}}'.format(longvar, query, longvar)))

@with_setup(_setup, _teardown)
def test_pyspark_livy_sql_options_spark2():
        query = "abc"
        sqlquery = SQLQuery(query, samplemethod='take', maxrows=120)

        assert_equals(sqlquery._pyspark_command("spark"),
                      Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).take(120): print({})'\
                              .format(LONG_RANDOM_VARIABLE_NAME, query,
                                      LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
        assert_equals(sqlquery._pyspark_command("spark"),
                      Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).collect(): print({})'\
                              .format(LONG_RANDOM_VARIABLE_NAME, query,
                                      LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sqlquery._pyspark_command("spark"),
                      Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.25).collect(): '
                              u'print({})'\
                              .format(LONG_RANDOM_VARIABLE_NAME, query,
                                      LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sqlquery._pyspark_command("spark"),
                      Command(u'import sys\nfor {} in spark.sql(u"""{} """).toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.33).take(3234): '
                              u'print({})'\
                              .format(LONG_RANDOM_VARIABLE_NAME, query,
                                      LONG_RANDOM_VARIABLE_NAME)))

@with_setup(_setup, _teardown)
def test_scala_livy_sql_options_spark2():
        query = "abc"
        sqlquery = SQLQuery(query, samplemethod='take', maxrows=100)

        assert_equals(sqlquery._scala_command("spark"),
                      Command('spark.sql("""{}""").toJSON.take(100).foreach(println)'.format(query)))

        sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
        assert_equals(sqlquery._scala_command("spark"),
                      Command('spark.sql("""{}""").toJSON.collect.foreach(println)'.format(query)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sqlquery._scala_command("spark"),
                      Command('spark.sql("""{}""").toJSON.sample(false, 0.25).collect.foreach(println)'.format(query)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sqlquery._scala_command("spark"),
                      Command('spark.sql("""{}""").toJSON.sample(false, 0.33).take(3234).foreach(println)'.format(query)))

@with_setup(_setup, _teardown)
def test_r_livy_sql_options_spark2():
        query = "abc"
        sqlquery = SQLQuery(query, samplemethod='take', maxrows=100)

        assert_equals(sqlquery._r_command("spark"),
                      Command('for ({} in (jsonlite:::toJSON(take(sql("{}"),100)))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='take', maxrows=-1)
        assert_equals(sqlquery._r_command("spark"),
                      Command('for ({} in (jsonlite:::toJSON(collect(sql("{}"))))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sqlquery._r_command("spark"),
                      Command('for ({} in (jsonlite:::toJSON(collect(sample(sql("{}"), FALSE, 0.25))))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, query, LONG_RANDOM_VARIABLE_NAME)))

        sqlquery = SQLQuery(query, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sqlquery._r_command("spark"),
                      Command('for ({} in (jsonlite:::toJSON(take(sample(sql("{}"), FALSE, 0.33),3234)))) {{cat({})}}'.format(LONG_RANDOM_VARIABLE_NAME, query, LONG_RANDOM_VARIABLE_NAME)))
