from nose.tools import with_setup, assert_equals

from remotespark.utils.constants import Constants
from remotespark.kernels.wrapperkernel.codetransformers import *


code = None
conn = None


def _setup():
    global code, conn

    code = "code"
    conn = "conn"


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_not_supported_transformer():
    transformer = NotSupportedTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("", code_to_run)
    assert_equals("Magic 'command' not supported.", error_to_show)
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_config_transformer_no_session():
    transformer = ConfigTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(False, conn, False, None, code)

    assert_equals("%spark config {}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_config_transformer_session_no_flag():
    transformer = ConfigTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("", code_to_run)
    assert error_to_show is not None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_config_transformer_session_flag():
    transformer = ConfigTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, True, None, code)

    assert_equals("%spark config {}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.delete_session_action)
    assert_equals(end_action, Constants.start_session_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_spark_transformer():
    transformer = SparkTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("%%spark\n{}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.start_session_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_sql_transformer():
    transformer = SqlTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("%%spark -c sql\n{}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.start_session_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_sql_transformer_output_var():
    transformer = SqlTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, "my_var", code)

    assert_equals("%%spark -c sql -o my_var\n{}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.start_session_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_hive_transformer():
    transformer = HiveTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("%%spark -c hive\n{}".format(code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.start_session_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_info_transformer():
    transformer = InfoTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("%spark info {}".format(conn), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_delete_transformer_no_force():
    transformer = DeleteSessionTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("", code_to_run)
    assert error_to_show is not None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_delete_transformer_force():
    transformer = DeleteSessionTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, True, None, code)

    assert_equals("%spark delete {} {}".format(conn, code), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, True)


@with_setup(_setup, _teardown)
def test_cleanup_transformer_no_force():
    transformer = CleanUpTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("", code_to_run)
    assert error_to_show is not None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_cleanup_transformer_force():
    transformer = CleanUpTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, True, None, code)

    assert_equals("%spark cleanup {}".format(conn), code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, True)


@with_setup(_setup, _teardown)
def test_logs_transformer_session():
    transformer = LogsTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(True, conn, False, None, code)

    assert_equals("%spark logs", code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_logs_transformer_no_session():
    transformer = LogsTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(False, conn, False, None, code)

    assert_equals("print('No logs yet.')", code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)


@with_setup(_setup, _teardown)
def test_python_transformer():
    transformer = PythonTransformer("command")

    code_to_run, error_to_show, begin_action, end_action, deletes_session = \
        transformer.get_code_to_execute(False, conn, False, None, code)

    assert_equals(code, code_to_run)
    assert error_to_show is None
    assert_equals(begin_action, Constants.do_nothing_action)
    assert_equals(end_action, Constants.do_nothing_action)
    assert_equals(deletes_session, False)
