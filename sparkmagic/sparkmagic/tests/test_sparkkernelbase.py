import asyncio


from unittest.mock import MagicMock, call, patch
from nose.tools import with_setup
from aiounittest import async_test, futurized

from sparkmagic.kernels.wrapperkernel.sparkkernelbase import SparkKernelBase
from sparkmagic.utils.constants import LANG_PYTHON

# AsyncMock is only available in Python >= 3.8
# This helps modify tests to accommodate older Python version
is_async_mock_available = True
try:
    # Python >= 3.8
    from unittest.mock import AsyncMock
except Exception:
    # Patch
    is_async_mock_available = False

    class AsyncMock(MagicMock):
        async def __call__(self, *args, **kwargs):
            return super(AsyncMock, self).__call__(*args, **kwargs)


kernel = None
execute_cell_mock = None
do_shutdown_mock = None
ipython_display = None
code = "some spark code"
user_code_parser = MagicMock(return_value=code)


class TestSparkKernel(SparkKernelBase):
    def __init__(self):
        kwargs = {"testing": True}
        super().__init__(
            None, None, None, None, None, LANG_PYTHON, user_code_parser, **kwargs
        )


def _setup():
    global kernel, execute_cell_mock, do_shutdown_mock, ipython_display

    kernel = TestSparkKernel()

    if is_async_mock_available:
        kernel._execute_cell_for_user = execute_cell_mock = AsyncMock(
            return_value={"test": "ing", "a": "b", "status": "ok"}
        )
        kernel._do_shutdown_ipykernel = do_shutdown_mock = AsyncMock()
    else:
        # Use futurized return values to keep async func behavior
        kernel._do_shutdown_ipykernel = do_shutdown_mock = MagicMock(
            return_value=futurized(None)
        )
        kernel._execute_cell_for_user = execute_cell_mock = MagicMock(
            return_value=futurized({"test": "ing", "a": "b", "status": "ok"})
        )

    kernel.ipython_display = ipython_display = MagicMock()


def _teardown():
    pass


# Helper for calling assert_called_once_with on native AsyncMock vs Mock w/ futurized return values
async def async_assert_called_once_with(mock, *values):
    result = mock.called_once_with(*values)
    if asyncio.iscoroutine(result):
        result = await result

    assert result


# Helper for asserting return values on native AsyncMock vs Mock w/ futurized return values
async def async_assert_return_value(mock, value):
    return_value = mock.return_value
    if asyncio.isfuture(return_value):
        return_value = await return_value

    assert return_value is value


@async_test
@with_setup(_setup, _teardown)
async def test_execute_valid_code():
    # Verify that the execution flows through.
    ret = await kernel.do_execute(code, False)

    user_code_parser.get_code_to_run.assert_called_once_with(code)
    await async_assert_return_value(execute_cell_mock, ret)
    assert kernel._fatal_error is None
    #  assert execute_cell_mock.called_once_with(code, True)
    await async_assert_called_once_with(execute_cell_mock, code, True)
    assert ipython_display.send_error.call_count == 0


@async_test
@with_setup(_setup, _teardown)
async def test_execute_throws_if_fatal_error_happened():
    # Verify that if a fatal error already happened, we don't run the code and show the fatal error instead.
    fatal_error = "Error."
    kernel._fatal_error = fatal_error

    ret = await kernel.do_execute(code, False)

    await async_assert_return_value(execute_cell_mock, ret)
    assert kernel._fatal_error == fatal_error
    #  assert execute_cell_mock.called_once_with("None", True)
    await async_assert_called_once_with(execute_cell_mock, "None", True)
    assert ipython_display.send_error.call_count == 1


@async_test
@with_setup(_setup, _teardown)
async def test_execute_alerts_user_if_an_unexpected_error_happens():
    # Verify that developer error shows developer error (the Github link).
    # Because do_execute is so minimal, we'll assume we have a bug in the _repeat_fatal_error method
    kernel._fatal_error = "Something bad happened before"
    kernel._repeat_fatal_error = AsyncMock(side_effect=ValueError)

    ret = await kernel.do_execute(code, False)
    await async_assert_return_value(execute_cell_mock, ret)
    #  assert execute_cell_mock.called_once_with("None", True)
    await async_assert_called_once_with(execute_cell_mock, "None", True)
    assert ipython_display.send_error.call_count == 1


@async_test
@with_setup(_setup, _teardown)
async def test_execute_throws_if_fatal_error_happens_for_execution():
    # Verify that the kernel sends the error from Python execution's context to the user
    fatal_error = u"Error."
    message = '{}\nException details:\n\t"{}"'.format(fatal_error, fatal_error)
    reply_content = dict()
    reply_content[u"status"] = u"error"
    reply_content[u"evalue"] = fatal_error
    execute_cell_mock.return_value = (
        reply_content if is_async_mock_available else futurized(reply_content)
    )

    ret = await kernel._execute_cell(
        code, False, shutdown_if_error=True, log_if_error=fatal_error
    )
    await async_assert_return_value(execute_cell_mock, ret)
    assert kernel._fatal_error == message
    #  assert execute_cell_mock.called_once_with("None", True)
    await async_assert_called_once_with(execute_cell_mock, "None", True)
    assert ipython_display.send_error.call_count == 1


@async_test
@with_setup(_setup, _teardown)
async def test_shutdown_cleans_up():
    # No restart
    kernel._execute_cell_for_user = ecfu_m = AsyncMock()
    kernel._do_shutdown_ipykernel = dsi_m = AsyncMock()

    await kernel.do_shutdown(False)

    ecfu_m.assert_called_once_with("%%_do_not_call_delete_session\n ", True, False)
    dsi_m.assert_called_once_with(False)

    # On restart
    kernel._execute_cell_for_user = ecfu_m = AsyncMock()
    kernel._do_shutdown_ipykernel = dsi_m = AsyncMock()

    await kernel.do_shutdown(True)

    ecfu_m.assert_called_once_with("%%_do_not_call_delete_session\n ", True, False)
    dsi_m.assert_called_once_with(True)


@async_test
@with_setup(_setup, _teardown)
async def test_register_auto_viz():
    await kernel._register_auto_viz()

    assert (
        call(
            "from autovizwidget.widget.utils import display_dataframe\nip = get_ipython()\nip.display_formatter"
            ".ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)",
            True,
            False,
            None,
            False,
        )
        in execute_cell_mock.mock_calls
    )


@async_test
@with_setup(_setup, _teardown)
async def test_change_language():
    await kernel._change_language()

    assert (
        call(
            "%%_do_not_call_change_language -l {}\n ".format(LANG_PYTHON),
            True,
            False,
            None,
            False,
        )
        in execute_cell_mock.mock_calls
    )


@async_test
@with_setup(_setup, _teardown)
async def test_load_magics():
    await kernel._load_magics_extension()

    assert (
        call("%load_ext sparkmagic.kernels", True, False, None, False)
        in execute_cell_mock.mock_calls
    )


@async_test
@with_setup(_setup, _teardown)
async def test_delete_session():
    await kernel._delete_session()

    assert (
        call("%%_do_not_call_delete_session\n ", True, False)
        in execute_cell_mock.mock_calls
    )


@async_test
@with_setup(_teardown)
async def test_execute_cell_for_user_ipykernel4():
    want = {"status": "OK"}
    # Can't use patch decorator because
    # it fails to patch async functions in Python < 3.8
    with patch(
        "ipykernel.ipkernel.IPythonKernel.do_execute",
        new_callable=MagicMock,
        return_value=want,
    ) as mock_ipy_execute:
        got = await TestSparkKernel()._execute_cell_for_user(code="1", silent=True)

        assert mock_ipy_execute.called
        assert want == got


@async_test
@with_setup(_teardown)
async def test_execute_cell_for_user_ipykernel5():
    want = {"status": "OK"}
    # Can't use patch decorator because
    # it fails to patch async functions in Python < 3.8
    with patch(
        "ipykernel.ipkernel.IPythonKernel.do_execute",
        new_callable=MagicMock,
    ) as mock_ipy_execute:
        mock_ipy_execute.return_value = asyncio.Future()
        mock_ipy_execute.return_value.set_result(want)

        got = await TestSparkKernel()._execute_cell_for_user(code="1", silent=True)

        assert mock_ipy_execute.called
        assert want == got


@async_test
@with_setup(_teardown)
async def test_execute_cell_for_user_ipykernel6():
    want = {"status": "OK"}
    # Can't use patch decorator because
    # it fails to patch async functions in Python < 3.8
    with patch(
        "ipykernel.ipkernel.IPythonKernel.do_execute", return_value=want
    ) as mock_ipy_execute:
        got = await TestSparkKernel()._execute_cell_for_user(code="1", silent=True)
        assert mock_ipy_execute.called
        assert want == got
