from mock import MagicMock
from nose.tools import assert_equals

from remotespark.utils.constants import Constants
from remotespark.magics.sparkmagicsbase import SparkMagicBase
from remotespark.livyclientlib.dataframeparseexception import DataFrameParseException


def test_get_livy_kind_covers_all_langs():
    for lang in Constants.lang_supported:
        SparkMagicBase.get_livy_kind(lang)


def test_print_endpoint_info_doesnt_throw():
    SparkMagicBase.print_endpoint_info(range(5))


def test_df_execution_without_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    method = MagicMock(return_value=df)
    cell = ""
    session = MagicMock()
    output_var = None

    res = magic.execute_against_context_that_returns_df(method, cell, session, output_var)

    method.assert_called_once_with(cell, session)
    assert res == df
    assert_equals(shell.user_ns.keys(), [])


def test_df_execution_with_output_var():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell

    df = 0
    method = MagicMock(return_value=df)
    cell = ""
    session = MagicMock()
    output_var = "var_name"

    res = magic.execute_against_context_that_returns_df(method, cell, session, output_var)

    method.assert_called_once_with(cell, session)
    assert res == df
    assert shell.user_ns[output_var] == df


def test_df_execution_throws():
    shell = MagicMock()
    shell.user_ns = {}
    magic = SparkMagicBase(None)
    magic.shell = shell
    error = "error"

    method = MagicMock(side_effect=DataFrameParseException(error))
    cell = ""
    session = MagicMock()
    output_var = "var_name"

    res = magic.execute_against_context_that_returns_df(method, cell, session, output_var)

    method.assert_called_once_with(cell, session)
    assert res == None
    assert_equals(shell.user_ns.keys(), [])
