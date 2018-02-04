# coding=utf-8

import pandas as pd
from mock import MagicMock
from sparkmagic.livyclientlib.exceptions import BadUserDataException
from nose.tools import assert_raises, assert_equals
from sparkmagic.livyclientlib.command import Command
import sparkmagic.utils.constants as constants
from sparkmagic.livyclientlib.sendpandasdftosparkcommand import SendPandasDfToSparkCommand

def test_send_to_scala():
    input_variable_name = 'input'
    input_variable_value = pd.DataFrame({'A': [1], 'B' : [2]})
    output_variable_name = 'output'
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, maxrows)
    sparkcommand._scala_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(constants.SESSION_KIND_SPARK, input_variable_name, input_variable_value, output_variable_name)
    sparkcommand._scala_command.assert_called_with(input_variable_name, input_variable_value, output_variable_name)

def test_send_to_r():
    input_variable_name = 'input'
    input_variable_value = pd.DataFrame({'A': [1], 'B' : [2]})
    output_variable_name = 'output'
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, maxrows)
    sparkcommand._r_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(constants.SESSION_KIND_SPARKR, input_variable_name, input_variable_value, output_variable_name)
    sparkcommand._r_command.assert_called_with(input_variable_name, input_variable_value, output_variable_name)

def test_send_to_python():
    input_variable_name = 'input'
    input_variable_value = pd.DataFrame({'A': [1], 'B' : [2]})
    output_variable_name = 'output'
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, maxrows)
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(constants.SESSION_KIND_PYSPARK, input_variable_name, input_variable_value, output_variable_name)
    sparkcommand._pyspark_command.assert_called_with(input_variable_name, input_variable_value, output_variable_name, python2=True)

def test_send_to_python3():
    input_variable_name = 'input'
    input_variable_value = pd.DataFrame({'A': [1], 'B' : [2]})
    output_variable_name = 'output'
    maxrows = 1
    sparkcommand = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, maxrows)
    sparkcommand._pyspark_command = MagicMock(return_value=MagicMock())
    sparkcommand.to_command(constants.SESSION_KIND_PYSPARK3, input_variable_name, input_variable_value, output_variable_name)
    sparkcommand._pyspark_command.assert_called_with(input_variable_name, input_variable_value, output_variable_name, python2=False)

def test_should_raise_when_input_is_not_pandas_df():
    input_variable_name = "input"
    input_variable_value = "not a pandas dataframe"
    output_variable_name = "output"
    sparkcommand = SendPandasDfToSparkCommand(input_variable_name, input_variable_value, output_variable_name, 1)
    assert_raises(BadUserDataException, sparkcommand.to_command, "spark", input_variable_name, input_variable_value, output_variable_name)
