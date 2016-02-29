from mock import MagicMock
from nose.tools import assert_equals

from remotespark.utils.constants import LANGS_SUPPORTED
from remotespark.utils.utils import get_livy_kind
from remotespark.magics.sparkmagicsbase import SparkMagicBase
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException
from remotespark.livyclientlib.sqlquery import SQLQuery


def test_get_livy_kind_covers_all_langs():
    for lang in LANGS_SUPPORTED:
        get_livy_kind(lang)


def test_print_endpoint_info_doesnt_throw():
    SparkMagicBase.print_endpoint_info(range(5))


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
    magic.spark_controller.run_cell_sql = MagicMock(return_value=df)

    res = magic.execute_sqlquery(query, session, output_var, False)

    magic.spark_controller.run_cell_sql.assert_called_once_with(query, session)
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
    magic.spark_controller.run_cell_sql = MagicMock(return_value=df)

    res = magic.execute_sqlquery(query, session, output_var, False)

    magic.spark_controller.run_cell_sql.assert_called_once_with(query, session)
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
    magic.spark_controller.run_cell_sql = MagicMock(return_value=df)

    res = magic.execute_sqlquery(cell, session, output_var, True)

    magic.spark_controller.run_cell_sql.assert_called_once_with(cell, session)
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
    magic.spark_controller.run_cell_sql = MagicMock(return_value=df)

    res = magic.execute_sqlquery(cell, session, output_var, True)

    magic.spark_controller.run_cell_sql.assert_called_once_with(cell, session)
    assert res is None
    assert shell.user_ns[output_var] == df


def test_df_execution_throws():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell
    error = "error"

    query = SQLQuery("")
    session = MagicMock()
    output_var = "var_name"

    magic.spark_controller = MagicMock()
    magic.spark_controller.run_cell_sql = MagicMock(side_effect=DataFrameParseException(error))

    res = magic.execute_sqlquery(query, session, output_var, False)

    magic.spark_controller.run_cell_sql.assert_called_once_with(query, session)
    assert res is None
    assert_equals(list(shell.user_ns.keys()), [])
