# coding=utf-8
import pytest
from mock import MagicMock, call
from pandas.testing import assert_frame_equal

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LONG_RANDOM_VARIABLE_NAME, MIMETYPE_TEXT_PLAIN
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException


backup_conf_defaults = None


def setup_function():
    global backup_conf_defaults
    backup_conf_defaults = {
        "samplemethod": conf.default_samplemethod(),
        "maxrows": conf.default_maxrows(),
        "samplefraction": conf.default_samplefraction(),
    }


def teardown_function():
    conf.override_all(backup_conf_defaults)


def test_to_command_pyspark():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand(variable_name)
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command("pyspark", variable_name)
    sparkcommand._pyspark_command.assert_called_with(variable_name)


def test_to_command_scala():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand(variable_name)
    sparkcommand._scala_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command("spark", variable_name)
    sparkcommand._scala_command.assert_called_with(variable_name)


def test_to_command_r():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand(variable_name)
    sparkcommand._r_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command("sparkr", variable_name)
    sparkcommand._r_command.assert_called_with(variable_name)


def test_to_command_invalid():
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand(variable_name)
    with pytest.raises(BadUserDataException):
        sparkcommand.to_command("invalid", variable_name)


def test_sparkstorecommand_initializes():
    variable_name = "var_name"
    samplemethod = "take"
    maxrows = 120
    samplefraction = 0.6
    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod, maxrows, samplefraction
    )
    assert sparkcommand.samplemethod == samplemethod
    assert sparkcommand.maxrows == maxrows
    assert sparkcommand.samplefraction == samplefraction


def test_sparkstorecommand_loads_defaults():
    defaults = {
        conf.default_samplemethod.__name__: "sample",
        conf.default_maxrows.__name__: 419,
        conf.default_samplefraction.__name__: 0.99,
    }
    conf.override_all(defaults)
    variable_name = "var_name"
    sparkcommand = SparkStoreCommand(variable_name)
    assert sparkcommand.samplemethod == defaults[conf.default_samplemethod.__name__]
    assert sparkcommand.maxrows == defaults[conf.default_maxrows.__name__]
    assert sparkcommand.samplefraction == defaults[conf.default_samplefraction.__name__]


def test_pyspark_livy_sampling_options():
    variable_name = "var_name"

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=120)
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).take(120): print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=-1)
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).collect(): print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.25, maxrows=-1
    )
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.25).collect(): "
        "print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.33, maxrows=3234
    )
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.33).take(3234): "
        "print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.33, maxrows=3234
    )
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).sample(False, 0.33).take(3234): "
        "print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod=None, maxrows=100)
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).take(100): print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod=None, maxrows=100)
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).take(100): print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )


def test_scala_livy_sampling_options():
    variable_name = "abc"

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=100)
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.take(100).foreach(println)".format(variable_name)
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=-1)
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.collect.foreach(println)".format(variable_name)
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.25, maxrows=-1
    )
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.sample(false, 0.25).collect.foreach(println)".format(variable_name)
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.33, maxrows=3234
    )
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.sample(false, 0.33).take(3234).foreach(println)".format(
            variable_name
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod=None, maxrows=100)
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.take(100).foreach(println)".format(variable_name)
    )


def test_r_livy_sampling_options():
    variable_name = "abc"

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=100)

    assert sparkcommand._r_command(variable_name) == Command(
        "for ({} in (jsonlite::toJSON(take({},100)))) {{cat({})}}".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=-1)
    assert sparkcommand._r_command(variable_name) == Command(
        "for ({} in (jsonlite::toJSON(collect({})))) {{cat({})}}".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.25, maxrows=-1
    )
    assert sparkcommand._r_command(variable_name) == Command(
        "for ({} in (jsonlite::toJSON(collect(sample({}, FALSE, 0.25))))) {{cat({})}}".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(
        variable_name, samplemethod="sample", samplefraction=0.33, maxrows=3234
    )
    assert sparkcommand._r_command(variable_name) == Command(
        "for ({} in (jsonlite::toJSON(take(sample({}, FALSE, 0.33),3234)))) {{cat({})}}".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )

    sparkcommand = SparkStoreCommand(variable_name, samplemethod=None, maxrows=100)
    assert sparkcommand._r_command(variable_name) == Command(
        "for ({} in (jsonlite::toJSON(take({},100)))) {{cat({})}}".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )


def test_execute_code():
    spark_events = MagicMock()
    variable_name = "abc"

    sparkcommand = SparkStoreCommand(
        variable_name, "take", 100, 0.2, spark_events=spark_events
    )
    sparkcommand.to_command = MagicMock(return_value=MagicMock())
    result = """{"z":100, "nullv":null, "y":50}
{"z":25, "nullv":null, "y":10}"""
    sparkcommand.to_command.return_value.execute = MagicMock(
        return_value=(True, result, MIMETYPE_TEXT_PLAIN)
    )
    session = MagicMock()
    session.kind = "pyspark"
    result = sparkcommand.execute(session)

    sparkcommand.to_command.assert_called_once_with(session.kind, variable_name)
    sparkcommand.to_command.return_value.execute.assert_called_once_with(session)


def test_unicode():
    variable_name = "collect 'è'"

    sparkcommand = SparkStoreCommand(variable_name, samplemethod="take", maxrows=120)
    assert sparkcommand._pyspark_command(variable_name) == Command(
        "import sys\nfor {} in {}.toJSON(use_unicode=(sys.version_info.major > 2)).take(120): print({})".format(
            LONG_RANDOM_VARIABLE_NAME, variable_name, LONG_RANDOM_VARIABLE_NAME
        )
    )
    assert sparkcommand._scala_command(variable_name) == Command(
        "{}.toJSON.take(120).foreach(println)".format(variable_name)
    )
