# coding=utf-8
from sparkmagic.livyclientlib.sendstringtosparkcommand import SendStringToSparkCommand
from mock import MagicMock
from sparkmagic.livyclientlib.exceptions import BadUserDataException
from nose.tools import assert_raises, assert_equals
from sparkmagic.livyclientlib.command import Command
import sparkmagic.utils.constants as constants


def test_send_to_scala():
    input_variable_name = "input"
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
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
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
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


def test_send_to_pyspark():
    input_variable_name = "input"
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
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


def test_to_command_invalid():
    input_variable_name = "input"
    input_variable_value = 42
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
    )
    assert_raises(
        BadUserDataException,
        sparkcommand.to_command,
        "invalid",
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )


def test_should_raise_when_input_aint_a_string():
    input_variable_name = "input"
    input_variable_value = 42
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
    )
    assert_raises(
        BadUserDataException,
        sparkcommand.to_command,
        "spark",
        input_variable_name,
        input_variable_value,
        output_variable_name,
    )


def test_should_create_a_valid_scala_expression():
    input_variable_name = "input"
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
    )
    assert_equals(
        sparkcommand._scala_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command('var {} = """{}"""'.format(output_variable_name, input_variable_value)),
    )


def test_should_create_a_valid_python_expression():
    input_variable_name = "input"
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
    )
    assert_equals(
        sparkcommand._pyspark_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command("{} = {}".format(output_variable_name, repr(input_variable_value))),
    )


def test_should_create_a_valid_r_expression():
    input_variable_name = "input"
    input_variable_value = "value"
    output_variable_name = "output"
    sparkcommand = SendStringToSparkCommand(
        input_variable_name, input_variable_value, output_variable_name
    )
    assert_equals(
        sparkcommand._r_command(
            input_variable_name, input_variable_value, output_variable_name
        ),
        Command(
            """assign("{}","{}")""".format(output_variable_name, input_variable_value)
        ),
    )
