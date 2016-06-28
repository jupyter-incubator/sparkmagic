# -*- coding: UTF-8 -*-
from mock import MagicMock
from nose.tools import assert_equals

from sparkmagic.utils.utils import get_livy_kind
from sparkmagic.utils.constants import LANGS_SUPPORTED, SESSION_KIND_PYSPARK, SESSION_KIND_SPARK, \
    IDLE_SESSION_STATUS, BUSY_SESSION_STATUS
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.livyclientlib.exceptions import DataFrameParseException
from sparkmagic.livyclientlib.sqlquery import SQLQuery


def test_load_emits_event():
    spark_events = MagicMock()

    SparkMagicBase(None, spark_events=spark_events)

    spark_events.emit_library_loaded_event.assert_called_once_with()


def test_get_livy_kind_covers_all_langs():
    for lang in LANGS_SUPPORTED:
        get_livy_kind(lang)


def test_df_execution_without_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    query = SQLQuery("")
    session = MagicMock()
    output_var = None

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, False)

    magic.spark_controller.run_sqlquery.assert_called_once_with(query, session)
    assert res == df
    assert_equals(list(shell.user_ns.keys()), [])


def test_df_execution_with_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    query = SQLQuery("")
    session = MagicMock()
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, False)

    magic.spark_controller.run_sqlquery.assert_called_once_with(query, session)
    assert res == df
    assert shell.user_ns[output_var] == df


def test_df_execution_quiet_without_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    cell = SQLQuery("")
    session = MagicMock()
    output_var = None

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, True)

    magic.spark_controller.run_sqlquery.assert_called_once_with(cell, session)
    assert res is None
    assert_equals(list(shell.user_ns.keys()), [])


def test_df_execution_quiet_with_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    cell = SQLQuery("")
    session = MagicMock()
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_sqlquery = MagicMock(return_value=df)

    res = magic.execute_sqlquery("", None, None, None, session, output_var, True)

    magic.spark_controller.run_sqlquery.assert_called_once_with(cell, session)
    assert res is None
    assert shell.user_ns[output_var] == df


def test_print_endpoint_info():
    magic = SparkMagicBase(None)
    magic.ipython_display = MagicMock()
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


def test_print_empty_endpoint_info():
    magic = SparkMagicBase(None)
    magic.ipython_display = MagicMock()
    current_session_id = None
    magic._print_endpoint_info([], current_session_id)
    magic.ipython_display.html.assert_called_once_with(u'No active sessions.')

