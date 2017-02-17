# coding=utf-8
from mock import MagicMock, call
from nose.tools import with_setup, assert_equals, assert_false, raises
import pandas as pd
from pandas.util.testing import assert_frame_equal

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LONG_RANDOM_VARIABLE_NAME
from sparkmagic.livyclientlib.sparkcommand import SparkStoreCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException


def _setup():
    pass
    

def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_to_command_pyspark():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand("Code", variable_name)
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command("pyspark", variable_name)
    sparkcommand._pyspark_command.assert_called_with(variable_name)


@with_setup(_setup, _teardown)
def test_to_command_pyspark3():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand("Code", variable_name)
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command("pyspark3", variable_name)
    sparkcommand._pyspark_command.assert_called_with(variable_name, False)

@with_setup(_setup, _teardown)
def test_sparkstorecommand_initializes():
    variable_name = "var_name"
    code = "HERE IS MY SPARK CODE ON RDDs"
    samplemethod = "take"
    maxrows = 120
    samplefraction = 0.6
    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod, maxrows, samplefraction)
    assert_equals(sparkcommand.code, code)
    assert_equals(sparkcommand.samplemethod, samplemethod)
    assert_equals(sparkcommand.maxrows, maxrows)
    assert_equals(sparkcommand.samplefraction, samplefraction)


@with_setup(_setup, _teardown)
def test_sparkstorecommand_loads_defaults():
    defaults = {
        conf.default_samplemethod.__name__: "sample",
        conf.default_maxrows.__name__: 419,
        conf.default_samplefraction.__name__: 0.99,
    }
    conf.override_all(defaults)
    variable_name = "var_name"
    code = "hugedataset.collect()"
    sparkcommand = SparkStoreCommand(code, variable_name)
    assert_equals(sparkcommand.code, code)
    assert_equals(sparkcommand.samplemethod, defaults[conf.default_samplemethod.__name__])
    assert_equals(sparkcommand.maxrows, defaults[conf.default_maxrows.__name__])
    assert_equals(sparkcommand.samplefraction, defaults[conf.default_samplefraction.__name__])

@with_setup(_setup, _teardown)
@raises(BadUserDataException)
def test_sparkstorecommand_rejects_bad_data():
    variable_name = "var_name"
    code = "hugedataset.collect()"
    samplemethod = "foo"
    _ = SparkStoreCommand(code, variable_name, samplemethod)


@with_setup(_setup, _teardown)
def test_pyspark_livy_sampling_options():
    variable_name = "abc"
    code = "hugedataset.collect()"
    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=120)
    print(sparkcommand._pyspark_command(variable_name).code)
    assert_equals(sparkcommand._pyspark_command(variable_name),
                  Command(u'for {} in {}.toJSON().take(120): print({}.encode("{}"))'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                  LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=-1)
    assert_equals(sparkcommand._pyspark_command(variable_name),
                  Command(u'for {} in {}.toJSON().collect(): print({}.encode("{}"))'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                  LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sparkcommand._pyspark_command(variable_name),
                  Command(u'for {} in {}.toJSON().sample(False, 0.25).collect(): '
                          u'print({}.encode("{}"))'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                  LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sparkcommand._pyspark_command(variable_name),
                  Command(u'for {} in {}.toJSON().sample(False, 0.33).take(3234): '
                          u'print({}.encode("{}"))'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                  LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sparkcommand._pyspark_command(variable_name, False),
                  Command(u'for {} in {}.toJSON().sample(False, 0.33).take(3234): '
                          u'print({})'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME)))

@with_setup(_setup, _teardown)
def test_scala_livy_sampling_options():
    variable_name = "abc"
    code = "hugedataset.collect()"
    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=100)
    assert_equals(sparkcommand._scala_command(variable_name),
                  Command('{}.toJSON.take(100).foreach(println)'.format(variable_name)))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=-1)
    assert_equals(sparkcommand._scala_command(variable_name),
                  Command('{}.toJSON.collect.foreach(println)'.format(variable_name)))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.25, maxrows=-1)
    assert_equals(sparkcommand._scala_command(variable_name),
                  Command('{}.toJSON.sample(false, 0.25).collect.foreach(println)'.format(variable_name)))

    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.33, maxrows=3234)
    assert_equals(sparkcommand._scala_command(variable_name),
                  Command('{}.toJSON.sample(false, 0.33).take(3234).foreach(println)'.format(variable_name)))


@with_setup(_setup, _teardown)
def test_execute_code():
    spark_events = MagicMock()
    variable_name = "abc"
    code = "hugedataset.collect()"
    sparkcommand = SparkStoreCommand(code, variable_name, "take", 100, 0.2, spark_events=spark_events)
    sparkcommand.to_command = MagicMock(return_value=MagicMock())
    result = """{"z":100, "nullv":null, "y":50}
{"z":25, "nullv":null, "y":10}"""
    sparkcommand.to_command.return_value.execute = MagicMock(return_value=(True, result))
    result_data = pd.DataFrame([{'z': 100, "nullv": None, 'y': 50}, {'z':25, "nullv":None, 'y':10}], columns=['z', "nullv", 'y'])
    session = MagicMock()
    session.kind = "pyspark"
    result = sparkcommand.execute(session)
    assert_frame_equal(result, result_data)
    sparkcommand.to_command.assert_called_once_with(session.kind, variable_name)
    sparkcommand.to_command.return_value.execute.assert_called_once_with(session)

@with_setup(_setup, _teardown)
def test_unicode():
    variable_name = u"collect 'è'"
    code = "hugedataset.collect()"
    sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=120)
    assert_equals(sparkcommand._pyspark_command(variable_name),
                  Command(u'for {} in {}.toJSON().take(120): print({}.encode("{}"))'\
                          .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                  LONG_RANDOM_VARIABLE_NAME, conf.pyspark_sql_encoding())))
    assert_equals(sparkcommand._scala_command(variable_name),
                  Command(u'{}.toJSON.take(120).foreach(println)'.format(variable_name)))


@with_setup(_setup, _teardown)
def test_pyspark_livy_sampling_options_spark2():
        variable_name = "abc"
        code = "hugedataset.collect()"
        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=120)

        assert_equals(sparkcommand._pyspark_command(variable_name),
                      Command(u'for {} in {}.toJSON().take(120): print({}.encode("{}"))'\
                              .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                      LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=-1)
        assert_equals(sparkcommand._pyspark_command(variable_name),
                      Command(u'for {} in {}.toJSON().collect(): print({}.encode("{}"))'\
                              .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                      LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sparkcommand._pyspark_command(variable_name),
                      Command(u'for {} in {}.toJSON().sample(False, 0.25).collect(): '
                              u'print({}.encode("{}"))'\
                              .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                      LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sparkcommand._pyspark_command(variable_name),
                      Command(u'for {} in {}.toJSON().sample(False, 0.33).take(3234): '
                              u'print({}.encode("{}"))'\
                              .format(LONG_RANDOM_VARIABLE_NAME, variable_name,
                                      LONG_RANDOM_VARIABLE_NAME, conf.pyspark_python_encoding())))

@with_setup(_setup, _teardown)
def test_scala_livy_sql_options_spark2():
        variable_name = "abc"
        code = "hugedataset.collect()"
        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=100)

        assert_equals(sparkcommand._scala_command(variable_name),
                      Command('{}.toJSON.take(100).foreach(println)'.format(variable_name)))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='take', maxrows=-1)
        assert_equals(sparkcommand._scala_command(variable_name),
                      Command('{}.toJSON.collect.foreach(println)'.format(variable_name)))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.25, maxrows=-1)
        assert_equals(sparkcommand._scala_command(variable_name),
                      Command('{}.toJSON.sample(false, 0.25).collect.foreach(println)'.format(variable_name)))

        sparkcommand = SparkStoreCommand(code, variable_name, samplemethod='sample', samplefraction=0.33, maxrows=3234)
        assert_equals(sparkcommand._scala_command(variable_name),
                      Command('{}.toJSON.sample(false, 0.33).take(3234).foreach(println)'.format(variable_name)))
