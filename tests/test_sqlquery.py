from nose.tools import with_setup, assert_equals, assert_false, raises

from remotespark.livyclientlib.sqlquery import SQLQuery
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
    assert_false(sqlquery.only_columns)


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
    assert_false(sqlquery.only_columns)


def test_sqlquery_only_columns():
    query = "HERE IS MY SQL QUERY SELECT * FROM CREATE DROP TABLE"
    samplemethod = "take"
    maxrows = 120
    samplefraction = 0.6
    sqlquery = SQLQuery(query, samplemethod, maxrows, samplefraction)
    assert_false(sqlquery.only_columns)
    sqlquery2 = SQLQuery.as_only_columns_query(sqlquery)

    sqlquery.only_columns = True
    assert_equals(sqlquery, sqlquery2)


@raises(AssertionError)
def test_sqlquery_rejects_bad_data():
    query = "HERE IS MY SQL QUERY SELECT * FROM CREATE DROP TABLE"
    samplemethod = "foo"
    _ = SQLQuery(query, samplemethod)
