# -*- coding: UTF-8 -*-

from mock import MagicMock
from nose.tools import assert_equals
from hdijupyterutils.constants import LANGS_SUPPORTED, SESSION_KIND_PYSPARK, SESSION_KIND_SPARK, \
    IDLE_SESSION_STATUS, BUSY_SESSION_STATUS
from hdijupyterutils.utils import get_livy_kind

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


def test_link():
    url = u"https://microsoft.com"
    magic = SparkMagicBase(None)
    assert_equals(magic._link(u'Link', url), u"""<a target="_blank" href="https://microsoft.com">Link</a>""")

    url = None
    assert_equals(magic._link(u'Link', url), u"")


def test_print_endpoint_info():
    magic = SparkMagicBase(None)
    magic.ipython_display = MagicMock()
    current_session_id = 1
    session1 = MagicMock()
    session1.id = 1
    session1.get_app_id.return_value = 'app1234'
    session1.kind = SESSION_KIND_PYSPARK
    session1.status = IDLE_SESSION_STATUS
    session1.get_spark_ui_url.return_value = 'https://microsoft.com/sparkui'
    session1.get_driver_log_url.return_value = 'https://microsoft.com/driverlog'
    session2 = MagicMock()
    session2.id = 3
    session2.get_app_id.return_value = 'app5069'
    session2.kind = SESSION_KIND_SPARK
    session2.status = BUSY_SESSION_STATUS
    session2.get_spark_ui_url.return_value = None
    session2.get_driver_log_url.return_value = None
    magic._print_endpoint_info([session2, session1], current_session_id)
    magic.ipython_display.html.assert_called_once_with(u"""<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr>\
<tr><td>1</td><td>app1234</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="https://microsoft.com/sparkui">Link</a></td><td><a target="_blank" href="https://microsoft.com/driverlog">Link</a></td><td>\u2714</td></tr>\
<tr><td>3</td><td>app5069</td><td>spark</td><td>busy</td><td></td><td></td><td></td></tr>\
</table>""")


def test_print_empty_endpoint_info():
    magic = SparkMagicBase(None)
    magic.ipython_display = MagicMock()
    current_session_id = None
    magic._print_endpoint_info([], current_session_id)
    magic.ipython_display.html.assert_called_once_with(u'No active sessions.')

