from mock import MagicMock
from nose.tools import with_setup, raises, assert_equals, assert_is
from IPython.core.magic import magics_class

from remotespark.kernels.kernelmagics import KernelMagics
from remotespark.livyclientlib.livyclienttimeouterror import LivyClientTimeoutError
from remotespark.livyclientlib.endpoint import Endpoint
from remotespark.livyclientlib.command import Command
from remotespark.livyclientlib.sqlquery import SQLQuery
import remotespark.utils.constants as constants
import remotespark.utils.configuration as conf

magic = None
spark_controller = None
shell = None
ipython_display = MagicMock()
spark_events = None


@magics_class
class TestKernelMagics(KernelMagics):
    def __init__(self, shell, data=None, spark_events=None):
        super(TestKernelMagics, self).__init__(shell, spark_events=spark_events)

        self.language = constants.LANG_PYTHON
        self.endpoint = Endpoint("url")

    def refresh_configuration(self):
        self.endpoint = Endpoint("new_url")


def _setup():
    global magic, spark_controller, shell, ipython_display, spark_events

    conf.override_all({})
    spark_events = MagicMock()

    magic = TestKernelMagics(shell=None, spark_events=spark_events)
    magic.shell = shell = MagicMock()
    magic.ipython_display = ipython_display = MagicMock()
    magic.spark_controller = spark_controller = MagicMock()
    magic._generate_uuid = MagicMock(return_value='0000')


def _teardown():
    pass


@with_setup(_setup, _teardown)
@raises(NotImplementedError)
def test_local():
    line = ""

    magic.local(line)


@with_setup(_setup, _teardown)
def test_start_session():
    line = ""
    assert not magic.session_started

    ret = magic._do_not_call_start_session(line)

    assert ret
    assert magic.session_started
    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})

    # Call a second time
    ret = magic._do_not_call_start_session(line)
    assert ret
    assert magic.session_started
    assert spark_controller.add_session.call_count == 1


@with_setup(_setup, _teardown)
def test_start_session_times_out():
    line = ""
    spark_controller.add_session = MagicMock(side_effect=LivyClientTimeoutError)
    assert not magic.session_started

    ret = magic._do_not_call_start_session(line)

    assert not ret
    assert magic.session_started
    assert magic.fatal_error
    assert_equals(ipython_display.send_error.call_count, 1)

    # Call after fatal error
    ipython_display.send_error.reset_mock()
    ret = magic._do_not_call_start_session(line)
    assert not ret
    assert magic.session_started
    assert_equals(ipython_display.send_error.call_count, 1)


@with_setup(_setup, _teardown)
def test_delete_session():
    line = ""
    magic.session_started = True

    magic._do_not_call_delete_session(line)

    assert not magic.session_started
    spark_controller.delete_session_by_name.assert_called_once_with(magic.session_name)

    # Call a second time
    magic._do_not_call_delete_session(line)
    assert not magic.session_started
    assert spark_controller.delete_session_by_name.call_count == 1


@with_setup(_setup, _teardown)
def test_change_language():
    language = constants.LANG_SCALA.upper()
    line = "-l {}".format(language)

    magic._do_not_call_change_language(line)

    assert_equals(constants.LANG_SCALA, magic.language)
    assert_equals(Endpoint("new_url"), magic.endpoint)


@with_setup(_setup, _teardown)
def test_change_language_session_started():
    language = constants.LANG_PYTHON
    line = "-l {}".format(language)
    magic.session_started = True

    magic._do_not_call_change_language(line)

    assert_equals(ipython_display.send_error.call_count, 1)
    assert_equals(constants.LANG_PYTHON, magic.language)
    assert_equals(Endpoint("url"), magic.endpoint)


@with_setup(_setup, _teardown)
def test_change_language_not_valid():
    language = "not_valid"
    line = "-l {}".format(language)

    magic._do_not_call_change_language(line)

    assert_equals(ipython_display.send_error.call_count, 1)
    assert_equals(constants.LANG_PYTHON, magic.language)
    assert_equals(Endpoint("url"), magic.endpoint)


@with_setup(_setup, _teardown)
def test_info():
    magic.print_endpoint_info = print_info_mock = MagicMock()
    line = ""
    session_info = ["1", "2"]
    spark_controller.get_all_sessions_endpoint_info = MagicMock(return_value=session_info)

    magic.info(line)

    print_info_mock.assert_called_once_with(session_info)
    spark_controller.get_session_id_for_client.assert_called_once_with(magic.session_name)
    _assert_magic_successful_event_omitted_once('info')


@with_setup(_setup, _teardown)
def test_help():
    magic.help("")

    assert_equals(ipython_display.html.call_count, 1)
    _assert_magic_successful_event_omitted_once('help')


@with_setup(_setup, _teardown)
def test_logs():
    logs = "logs"
    line = ""

    magic.logs(line)
    ipython_display.write.assert_called_once_with("No logs yet.")
    _assert_magic_successful_event_omitted_once('logs')

    ipython_display.write.reset_mock()

    magic.session_started = True

    spark_controller.get_logs = MagicMock(return_value=(True, logs))
    magic.logs(line)
    ipython_display.write.assert_called_once_with(logs)



    spark_controller.get_logs = MagicMock(return_value=(False, logs))
    magic.logs(line)
    ipython_display.send_error.assert_called_once_with(logs)


@with_setup(_setup, _teardown)
def test_configure():
    # Mock info method
    magic.info = MagicMock()

    # Session not started
    conf.override_all({})
    magic.configure('', '{"extra": "yes"}')
    assert conf.session_configs() == {"extra": "yes"}
    _assert_magic_successful_event_omitted_once('configure')
    magic.info.assert_called_once_with("")

    # Session started - no -f
    magic.session_started = True
    conf.override_all({})
    magic.configure('', "{\"extra\": \"yes\"}")

    assert conf.session_configs() == {}
    assert_equals(ipython_display.send_error.call_count, 1)

    # Session started - with -f
    magic.info.reset_mock()
    conf.override_all({})
    magic.configure("-f", "{\"extra\": \"yes\"}")
    assert conf.session_configs() == {"extra": "yes"}
    spark_controller.delete_session_by_name.assert_called_once_with(magic.session_name)
    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK, "extra": "yes"})
    magic.info.assert_called_once_with("")


@with_setup(_setup, _teardown)
def test_get_session_settings():
    assert magic.get_session_settings("something", False) == "something"
    assert magic.get_session_settings("something    ", False) == "something"
    assert magic.get_session_settings("    something", False) == "something"
    assert magic.get_session_settings("-f something", True) == "something"
    assert magic.get_session_settings("something -f", True) == "something"
    assert magic.get_session_settings("something", True) is None


@with_setup(_setup, _teardown)
def test_spark():
    line = ""
    cell = "some spark code"
    spark_controller.run_command = MagicMock(return_value=(True, line))

    magic.spark(line, cell)
    _assert_magic_successful_event_omitted_once('spark')

    ipython_display.write.assert_called_once_with(line)
    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    spark_controller.run_command.assert_called_once_with(Command(cell))


@with_setup(_setup, _teardown)
def test_spark_error():
    line = ""
    cell = "some spark code"
    spark_controller.run_command = MagicMock(return_value=(False, line))

    magic.spark(line, cell)

    ipython_display.send_error.assert_called_once_with(line)
    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    spark_controller.run_command.assert_called_once_with(Command(cell))


@with_setup(_setup, _teardown)
def test_spark_failed_session_start():
    line = ""
    cell = "some spark code"
    magic._do_not_call_start_session = MagicMock(return_value=False)

    ret = magic.spark(line, cell)

    assert_is(ret, None)
    assert_equals(ipython_display.write.call_count, 0)
    assert_equals(spark_controller.add_session.call_count, 0)
    assert_equals(spark_controller.run_command.call_count, 0)


@with_setup(_setup, _teardown)
def test_sql_without_output():
    line = ""
    cell = "some spark code"
    magic.execute_sqlquery = MagicMock()

    magic.sql(line, cell)
    _assert_magic_successful_event_omitted_once('sql')

    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    magic.execute_sqlquery.assert_called_once_with(SQLQuery(cell), None, None, False)


@with_setup(_setup, _teardown)
def test_sql_with_output():
    line = "-o my_var"
    cell = "some spark code"
    magic.execute_sqlquery = MagicMock()

    magic.sql(line, cell)

    _assert_magic_successful_event_omitted_once('sql')

    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    magic.execute_sqlquery.assert_called_once_with(SQLQuery(cell), None, "my_var", False)


@with_setup(_setup, _teardown)
def test_sql_failed_session_start():
    line = ""
    cell = "some spark code"
    magic._do_not_call_start_session = MagicMock(return_value=False)

    ret = magic.sql(line, cell)

    _assert_magic_successful_event_omitted_once('sql')

    assert_is(ret, None)
    assert_equals(spark_controller.add_session.call_count, 0)
    assert_equals(spark_controller.execute_sqlquery.call_count, 0)


@with_setup(_setup, _teardown)
def test_sql_quiet():
    line = "-q -o Output"
    cell = ""
    magic.execute_sqlquery = MagicMock()

    ret = magic.sql(line, cell)
    _assert_magic_successful_event_omitted_once('sql')

    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    magic.execute_sqlquery.assert_called_once_with(SQLQuery(cell), None, "Output", True)


@with_setup(_setup, _teardown)
def test_sql_sample_options():
    line = "-q -m sample -n 142 -r 0.3"
    cell = ""
    magic.execute_sqlquery = MagicMock()

    ret = magic.sql(line, cell)
    _assert_magic_successful_event_omitted_once('sql')

    spark_controller.add_session.assert_called_once_with(magic.session_name, magic.endpoint, False,
                                                         {"kind": constants.SESSION_KIND_PYSPARK})
    magic.execute_sqlquery.assert_called_once_with(SQLQuery(cell, samplemethod="sample",
                                                            maxrows=142, samplefraction=0.3),
                                                   None, None, True)


@with_setup(_setup, _teardown)
def test_cleanup_without_force():
    line = ""
    cell = ""
    magic.session_started = True
    spark_controller.cleanup_endpoint = MagicMock()
    spark_controller.delete_session_by_name = MagicMock()

    magic.cleanup(line, cell)

    _assert_magic_successful_event_omitted_once('cleanup')

    assert_equals(ipython_display.send_error.call_count, 1)
    assert_equals(spark_controller.cleanup_endpoint.call_count, 0)


@with_setup(_setup, _teardown)
def test_cleanup_with_force():
    line = "-f"
    cell = ""
    magic.session_started = True
    spark_controller.cleanup_endpoint = MagicMock()
    spark_controller.delete_session_by_name = MagicMock()

    magic.cleanup(line, cell)

    _assert_magic_successful_event_omitted_once('cleanup')

    spark_controller.cleanup_endpoint.assert_called_once_with(magic.endpoint)
    spark_controller.delete_session_by_name.assert_called_once_with(magic.session_name)


@with_setup(_setup, _teardown)
def test_delete_without_force():
    session_id = "0"
    line = "-s {}".format(session_id)
    cell = ""
    spark_controller.delete_session_by_id = MagicMock()
    spark_controller.get_session_id_for_client = MagicMock(return_value=session_id)

    magic.delete(line, cell)

    _assert_magic_successful_event_omitted_once('delete')

    assert_equals(ipython_display.send_error.call_count, 1)
    assert_equals(spark_controller.delete_session_by_id.call_count, 0)


@with_setup(_setup, _teardown)
def test_delete_with_force_same_session():
    session_id = "0"
    line = "-f -s {}".format(session_id)
    cell = ""
    spark_controller.delete_session_by_id = MagicMock()
    spark_controller.get_session_id_for_client = MagicMock(return_value=session_id)

    magic.delete(line, cell)

    _assert_magic_successful_event_omitted_once('delete')

    assert_equals(ipython_display.send_error.call_count, 1)
    assert_equals(spark_controller.delete_session_by_id.call_count, 0)


@with_setup(_setup, _teardown)
def test_delete_with_force_none_session():
    # This happens when session has not been created
    session_id = "0"
    line = "-f -s {}".format(session_id)
    cell = ""
    spark_controller.delete_session_by_id = MagicMock()
    spark_controller.get_session_id_for_client = MagicMock(return_value=None)

    magic.delete(line, cell)

    _assert_magic_successful_event_omitted_once('delete')

    spark_controller.get_session_id_for_client.assert_called_once_with(magic.session_name)
    spark_controller.delete_session_by_id.assert_called_once_with(magic.endpoint, session_id)


@with_setup(_setup, _teardown)
def test_delete_with_force_different_session():
    # This happens when session has not been created
    session_id = "0"
    line = "-f -s {}".format(session_id)
    cell = ""
    spark_controller.delete_session_by_id = MagicMock()
    spark_controller.get_session_id_for_client = MagicMock(return_value="1")

    magic.delete(line, cell)

    _assert_magic_successful_event_omitted_once('delete')

    spark_controller.get_session_id_for_client.assert_called_once_with(magic.session_name)
    spark_controller.delete_session_by_id.assert_called_once_with(magic.endpoint, session_id)

def _assert_magic_successful_event_omitted_once(name):
    magic._generate_uuid.assert_called_once_with()
    spark_events.emit_magic_execution_start_event.assert_called_once_with(name,
                                                                                 constants.SESSION_KIND_PYSPARK,
                                                                                 magic._generate_uuid.return_value)
    spark_events.emit_magic_execution_end_event.assert_called_once_with(name, constants.SESSION_KIND_PYSPARK,
                                                                               magic._generate_uuid.return_value, True,
                                                                               '', '')
