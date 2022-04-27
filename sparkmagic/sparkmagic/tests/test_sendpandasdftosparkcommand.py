# coding=utf-8

import pandas as pd
from mock import MagicMock
from sparkmagic.livyclientlib.exceptions import BadUserDataException
from nose.tools import assert_raises, assert_equals
from sparkmagic.livyclientlib.command import Command
import sparkmagic.utils.constants as constants
from sparkmagic.livyclientlib.sendpandasdftosparkcommand import (
    SendPandasDfToSparkCommand,
)


def test_send_to_scala():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, maxrows
    )
    sparkcommand._scala_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(
        constants.SESSION_KIND_SPARK,
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )
    sparkcommand._scala_command.assert_called_with(
        input_variable_name, input_variable_value, output_variable_name
    )


def test_send_to_r():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, maxrows
    )
    sparkcommand._r_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(
        constants.SESSION_KIND_SPARKR,
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )
    sparkcommand._r_command.assert_called_with(
        input_variable_name, input_variable_value, output_variable_name
    )


def test_send_to_python():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, maxrows
    )
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(
        constants.SESSION_KIND_PYSPARK,
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )
    sparkcommand._pyspark_command.assert_called_with(
        input_variable_name, input_variable_value, output_variable_name
    )


def test_should_create_a_valid_scala_expression():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"

    pandas_df_jsonized = """[{"A":1,"B":2}]"""
    expected_scala_code = '''
        val rdd_json_array = spark.sparkContext.makeRDD("""{}""" :: Nil)
        val {} = spark.read.json(rdd_json_array)'''.format(
        pandas_df_jsonized, output_variable_name
    )

    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, 1
    )
    assert_equals(
        sparkcommand._scala_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(expected_scala_code),
    )


def test_should_create_a_valid_r_expression():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"

    pandas_df_jsonized = """[{"A":1,"B":2}]"""
    expected_r_code = """
        fileConn<-file("temporary_pandas_df_sparkmagics.txt")
        writeLines('{}', fileConn)
        close(fileConn)
        {} <- read.json("temporary_pandas_df_sparkmagics.txt")
        {}.persist()
        file.remove("temporary_pandas_df_sparkmagics.txt")""".format(
        pandas_df_jsonized, output_variable_name, output_variable_name
    )

    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, 1
    )
    assert_equals(
        sparkcommand._r_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(expected_r_code),
    )


def test_should_create_a_valid_python3_expression():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"
    pandas_df_jsonized = """[{"A":1,"B":2}]"""

    expected_python3_code = SendPandasDfToSparkCommand._python_decode

    expected_python3_code += """
        json_array = json_loads_byteified('{}')
        rdd_json_array = spark.sparkContext.parallelize(json_array)
        {} = spark.read.json(rdd_json_array)""".format(
        pandas_df_jsonized, output_variable_name
    )

    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, 1
    )
    assert_equals(
        sparkcommand._pyspark_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(expected_python3_code),
    )


def test_should_create_a_valid_python2_expression():
    input_variable_name = "input"
    input_variable_value = pd.DataFrame({"A": [1], "B": [2]})
    output_variable_name = "output"
    pandas_df_jsonized = """[{"A":1,"B":2}]"""

    expected_python2_code = SendPandasDfToSparkCommand._python_decode

    expected_python2_code += """
        json_array = json_loads_byteified('{}')
        rdd_json_array = spark.sparkContext.parallelize(json_array)
        {} = spark.read.json(rdd_json_array)""".format(
        pandas_df_jsonized, output_variable_name
    )

    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, 1
    )
    assert_equals(
        sparkcommand._pyspark_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(expected_python2_code),
    )


def test_should_properly_limit_pandas_dataframe():
    input_variable_name = "input"
    max_rows = 1
    input_variable_value = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [5, 6, 7, 8, 9]})
    output_variable_name = "output"

    pandas_df_jsonized = (
        """[{"A":0,"B":5}]"""  # notice we expect json to have dropped all but one row
    )
    expected_scala_code = '''
        val rdd_json_array = spark.sparkContext.makeRDD("""{}""" :: Nil)
        val {} = spark.read.json(rdd_json_array)'''.format(
        pandas_df_jsonized, output_variable_name
    )

    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, max_rows
    )
    assert_equals(
        sparkcommand._scala_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(expected_scala_code),
    )


def test_should_raise_when_input_is_not_pandas_df():
    input_variable_name = "input"
    input_variable_value = "not a pandas dataframe"
    output_variable_name = "output"
    sparkcommand = SendPandasDfToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name, 1
    )
    assert_raises(
        BadUserDataException,
        sparkcommand.to_command,
        "spark",
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )
