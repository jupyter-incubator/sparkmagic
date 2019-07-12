import ipykernel
from mock import MagicMock, call, patch
from nose.tools import with_setup

from sparkmagic.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase
from sparkmagic.utils.constants import LANG_PYTHON

kernel = None
execute_cell_mock = None
do_shutdown_mock = None
ipython_display = None
code = "some spark code"
user_code_parser = MagicMock(return_value=code)


class TestSparkKernel(SparkKernelBase):
    def __init__(self):
        kwargs = {"testing": True}
        super(TestSparkKernel, self).__init__(None, None, None, None, None, LANG_PYTHON, user_code_parser,
                                              **kwargs)


def _setup():
    global kernel, execute_cell_mock, do_shutdown_mock, ipython_display

    kernel = TestSparkKernel()

    kernel._execute_cell_for_user = execute_cell_mock = MagicMock(return_value={'test': 'ing', 'a': 'b',
                                                                                'status': 'ok'})
    kernel._do_shutdown_ipykernel = do_shutdown_mock = MagicMock()
    kernel.ipython_display = ipython_display = MagicMock()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_execute_valid_code():
    # Verify that the execution flows through.
    ret = kernel.do_execute(code, False)

    user_code_parser.get_code_to_run.assert_called_once_with(code)
    assert ret is execute_cell_mock.return_value
    assert kernel._fatal_error is None
    assert execute_cell_mock.called_once_with(code, True)
    assert ipython_display.send_error.call_count == 0


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happened():
    # Verify that if a fatal error already happened, we don't run the code and show the fatal error instead.
    fatal_error = "Error."
    kernel._fatal_error = fatal_error

    ret = kernel.do_execute(code, False)

    assert ret is execute_cell_mock.return_value
    assert kernel._fatal_error == fatal_error
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_execute_alerts_user_if_an_unexpected_error_happens():
    # Verify that developer error shows developer error (the Github link).
    # Because do_execute is so minimal, we'll assume we have a bug in the _repeat_fatal_error method
    kernel._fatal_error = "Something bad happened before"
    kernel._repeat_fatal_error = MagicMock(side_effect=ValueError)

    ret = kernel.do_execute(code, False)

    assert ret is execute_cell_mock.return_value
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_execute_throws_if_fatal_error_happens_for_execution():
    # Verify that the kernel sends the error from Python execution's context to the user
    fatal_error = u"Error."
    message = "{}\nException details:\n\t\"{}\"".format(fatal_error, fatal_error)
    reply_content = dict()
    reply_content[u"status"] = u"error"
    reply_content[u"evalue"] = fatal_error
    execute_cell_mock.return_value = reply_content

    ret = kernel._execute_cell(code, False, shutdown_if_error=True, log_if_error=fatal_error)

    assert ret is execute_cell_mock.return_value
    assert kernel._fatal_error == message
    assert execute_cell_mock.called_once_with("None", True)
    assert ipython_display.send_error.call_count == 1


@with_setup(_setup, _teardown)
def test_shutdown_cleans_up():
    # No restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()

    kernel.do_shutdown(False)

    ecfu_m.assert_called_once_with("%%_do_not_call_delete_session\n ", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel._execute_cell_for_user = ecfu_m = MagicMock()
    kernel._do_shutdown_ipykernel = dsi_m = MagicMock()

    kernel.do_shutdown(True)

    ecfu_m.assert_called_once_with("%%_do_not_call_delete_session\n ", True, False)
    dsi_m.assert_called_once_with(True)


@with_setup(_setup, _teardown)
def test_register_auto_viz():
    kernel._register_auto_viz()

    assert call("from autovizwidget.widget.utils import display_dataframe\nip = get_ipython()\nip.display_formatter"
                ".ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)",
                True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_change_language():
    kernel._change_language()

    assert call("%%_do_not_call_change_language -l {}\n ".format(LANG_PYTHON),
                True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_load_magics():
    kernel._load_magics_extension()

    assert call("%load_ext sparkmagic.kernels", True, False, None, False) in execute_cell_mock.mock_calls


@with_setup(_setup, _teardown)
def test_delete_session():
    kernel._delete_session()

    assert call("%%_do_not_call_delete_session\n ", True, False) in execute_cell_mock.mock_calls

@patch.object(ipykernel.ipkernel.IPythonKernel, 'do_execute')
@with_setup(_teardown)
def test_execute_cell_for_user_ipykernel5(mock_ipy_execute):
    import sys
    if sys.version_info.major == 2:
        from unittest import SkipTest
        raise SkipTest("Python 3 only")
    else:
        import asyncio
    mock_ipy_execute_result = asyncio.Future()
    mock_ipy_execute_result.set_result({'status': 'OK'})
    mock_ipy_execute.return_value = mock_ipy_execute_result

    actual_result = TestSparkKernel()._execute_cell_for_user(code='Foo', silent=True)

    assert {'status': 'OK'} == actual_result


@patch.object(ipykernel.ipkernel.IPythonKernel, 'do_execute')
@with_setup(_teardown)
def test_execute_cell_for_user_ipykernel4(mock_ipy_execute):
    mock_ipy_execute.return_value = {'status': 'OK'}

    actual_result = TestSparkKernel()._execute_cell_for_user(code='Foo', silent=True)

    assert {'status': 'OK'} == actual_result
