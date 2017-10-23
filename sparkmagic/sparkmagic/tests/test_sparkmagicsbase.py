# -*- coding: UTF-8 -*-
from mock import MagicMock
from nose.tools import with_setup, assert_equals, assert_raises, raises

from sparkmagic.utils.configuration import get_livy_kind
from sparkmagic.utils.constants import LANGS_SUPPORTED, SESSION_KIND_PYSPARK, SESSION_KIND_SPARK, \
    IDLE_SESSION_STATUS, BUSY_SESSION_STATUS
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.livyclientlib.exceptions import DataFrameParseException, BadUserDataException
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand

def _setup():
    global magic, session, shell, ipython_display
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell
    session = MagicMock()
    magic.spark_controller = MagicMock()
    magic.ipython_display = MagicMock()

def _teardown():
    pass


def test_load_emits_event():
    spark_events = MagicMock()
    SparkMagicBase(None, spark_events=spark_events)
    spark_events.emit_library_loaded_event.assert_called_once_with()


def test_get_livy_kind_covers_all_langs():
    for lang in LANGS_SUPPORTED:
        get_livy_kind(lang)


@with_setup(_setup, _teardown)
def test_sql_df_execution_without_output_var():
    df = 0
    query = SQLQuery("")
    output_var = None
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)
    res = magic.execute_sqlquery("", None, None, None, session, output_var, False, None)

    magic.spark_controller.run_sqlquery.assert_called_once_with(query, session)
    assert res == df
    assert_equals(list(shell.user_ns.keys()), [])


@with_setup(_setup, _teardown)
def test_sql_df_execution_with_output_var():
    df = 0
    query = SQLQuery("")
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, False, None)

    magic.spark_controller.run_sqlquery.assert_called_once_with(query, session)
    assert res == df
    assert shell.user_ns[output_var] == df


@with_setup(_setup, _teardown)
def test_sql_df_execution_quiet_without_output_var():
    df = 0
    cell = SQLQuery("")
    output_var = None

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, True, None)

    magic.spark_controller.run_sqlquery.assert_called_once_with(cell, session)
    assert res is None
    assert_equals(list(shell.user_ns.keys()), [])


@with_setup(_setup, _teardown)
def test_sql_df_execution_quiet_with_output_var():
    df = 0
    cell = SQLQuery("")
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, True, None)

    magic.spark_controller.run_sqlquery.assert_called_once_with(cell, session)
    assert res is None
    assert shell.user_ns[output_var] == df


@with_setup(_setup, _teardown)
def test_sql_df_execution_quiet_with_coerce():
    df = 0
    cell = SQLQuery("", coerce=True)
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, True, True)

    magic.spark_controller.run_sqlquery.assert_called_once_with(cell, session)
    assert res is None
    assert shell.user_ns[output_var] == df


@with_setup(_setup, _teardown)
def test_print_endpoint_info():
    current_session_id = 1
    session1 = MagicMock()
    session1.id = 1
    session1.get_row_html.return_value = u"""<tr><td>row1</td></tr>"""
    session2 = MagicMock()
    session2.id = 3
    session2.get_row_html.return_value = u"""<tr><td>row2</td></tr>"""
    magic._print_endpoint_info([session2, session1], current_session_id)
    magic.ipython_display.html.assert_called_once_with(u"""<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr>\
<tr><td>row1</td></tr><tr><td>row2</td></tr>\
</table>""")


@with_setup(_setup, _teardown)
def test_print_empty_endpoint_info():
    current_session_id = None
    magic._print_endpoint_info([], current_session_id)
    magic.ipython_display.html.assert_called_once_with(u'No active sessions.')


@with_setup(_setup, _teardown)
@raises(BadUserDataException)
def test_should_raise_when_variable_value_is_none():
    input_variable_name = "x_in"
    output_variable_name = "x_out"
    var_type = "str"
    magic.shell.user_ns[input_variable_name] = None

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

@with_setup(_setup, _teardown)
@raises(BadUserDataException)
def test_should_raise_when_type_is_incorrect():
    input_variable_name = "x_in"
    input_variable_value = "x_value"
    output_variable_name = "x_out"
    var_type = "incorrect"
    magic.shell.user_ns[input_variable_name] = input_variable_value

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

@with_setup(_setup, _teardown)
def test_should_print_error_when_str_command_failed():
    input_variable_name = "x_in"
    input_variable_value = "x_value"
    output_variable_name = "x_out"
    var_type = "STR"
    output_value = "error"
    magic.shell.user_ns[input_variable_name] = input_variable_value
    magic.spark_controller.run_command.return_value = (False, output_value)

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

    magic.ipython_display.send_error.assert_called_once_with(output_value)
    assert not magic.ipython_display.write.called

@with_setup(_setup, _teardown)
def test_should_print_error_when_df_command_failed():
    input_variable_name = "x_in"
    input_variable_value = "x_value"
    output_variable_name = "x_out"
    var_type = "df"
    output_value = "error"
    magic.shell.user_ns[input_variable_name] = input_variable_value
    magic.spark_controller.run_command.return_value = (False, output_value)

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

    magic.ipython_display.send_error.assert_called_once_with(output_value)
    assert not magic.ipython_display.write.called

@with_setup(_setup, _teardown)
def should_name_the_output_variable_the_same_as_input_name_when_custom_name_not_provided():
    input_variable_name = "x_in"
    input_variable_value = output_value = "x_value"
    var_type = "str"
    output_variable_name = None
    magic.shell.user_ns[input_variable_name] = input_variable_value
    magic.spark_controller.run_command.return_value = (True, output_value)
    expected_message = u'Successfully passed \'{}\' as \'{}\' to Spark kernel'.format(input_variable_name, input_variable_name)

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

    magic.ipython_display.write.assert_called_once_with(expected_message)
    assert not magic.ipython_display.send_error.called

@with_setup(_setup, _teardown)
def should_write_successfully_when_everything_is_correct():
    input_variable_name = "x_in"
    input_variable_value = output_value = "x_value"
    output_variable_name = "x_out"
    var_type = "str"
    magic.shell.user_ns[input_variable_name] = input_variable_value
    magic.spark_controller.run_command.return_value = (True, output_value)
    expected_message = u'Successfully passed \'{}\' as \'{}\' to Spark kernel'.format(input_variable_name, output_variable_name)

    magic.send_to_spark("", input_variable_name, var_type, output_variable_name, None)

    magic.ipython_display.write.assert_called_once_with(expected_message)
    assert not magic.ipython_display.send_error.called

@with_setup(_setup, _teardown)
def test_spark_execution_without_output_var():
    output_var = None
    
    magic.spark_controller.run_command.return_value = (True,'out')
    magic.execute_spark("", output_var, None, None, None, session, None)
    magic.ipython_display.write.assert_called_once_with('out')
    assert not magic.spark_controller._spark_store_command.called

    magic.spark_controller.run_command.return_value = (False,'out')
    magic.execute_spark("", output_var, None, None, None, session, None)
    magic.ipython_display.send_error.assert_called_once_with('out')
    assert not magic.spark_controller._spark_store_command.called

@with_setup(_setup, _teardown)
def test_spark_execution_with_output_var():
    mockSparkCommand = MagicMock()
    magic._spark_store_command = MagicMock(return_value=mockSparkCommand)
    output_var = "var_name"
    df = 'df'

    magic.spark_controller.run_command.side_effect = [(True,'out'), df]
    magic.execute_spark("", output_var, None, None, None, session, True)
    magic.ipython_display.write.assert_called_once_with('out')
    magic._spark_store_command.assert_called_once_with(output_var, None, None, None, True)
    assert shell.user_ns[output_var] == df

    magic.spark_controller.run_command.side_effect = None
    magic.spark_controller.run_command.return_value = (False,'out')
    magic.execute_spark("", output_var, None, None, None, session, True)
    magic.ipython_display.send_error.assert_called_once_with('out')


@with_setup(_setup, _teardown)
def test_spark_exception_with_output_var():
    mockSparkCommand = MagicMock()
    magic._spark_store_command = MagicMock(return_value=mockSparkCommand)
    exception = BadUserDataException("Ka-boom!")
    output_var = "var_name"
    df = 'df'

    magic.spark_controller.run_command.side_effect = [(True,'out'), exception]
    assert_raises(BadUserDataException, magic.execute_spark,"", output_var, None, None, None, session, True)
    magic.ipython_display.write.assert_called_once_with('out')
    magic._spark_store_command.assert_called_once_with(output_var, None, None, None, True)
    assert shell.user_ns == {}
