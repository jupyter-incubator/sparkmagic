# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import asyncio
import inspect
import requests


from ipykernel.ipkernel import IPythonKernel
from hdijupyterutils.ipythondisplay import IpythonDisplay

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.livyclientlib.exceptions import wrap_unexpected_exceptions
from sparkmagic.kernels.wrapperkernel.usercodeparser import UserCodeParser

# NOTE: This is a (hopefully) temporary workaround to accommodate async do_execute in ipykernel>=6
import nest_asyncio


# NOTE: This is a (hopefully) temporary workaround to accommodate async do_execute in ipykernel>=6
# Taken from: https://github.com/jupyter/notebook/blob/eb3a1c24839205afcef0ba65ace2309d38300a2b/notebook/utils.py#L332
def run_sync(maybe_async):
    """If async, runs maybe_async and blocks until it has executed,
    possibly creating an event loop.
    If not async, just returns maybe_async as it is the result of something
    that has already executed.
    Parameters
    ----------
    maybe_async : async or non-async object
        The object to be executed, if it is async.
    Returns
    -------
    result :
        Whatever the async object returns, or the object itself.
    """
    if not inspect.isawaitable(maybe_async):
        # that was not something async, just return it
        return maybe_async
    # it is async, we need to run it in an event loop

    def wrapped():
        create_new_event_loop = False
        result = None
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            create_new_event_loop = True
        else:
            if loop.is_closed():
                create_new_event_loop = True
        if create_new_event_loop:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(maybe_async)
        except RuntimeError as e:
            if str(e) == "This event loop is already running":
                # just return a Future, hoping that it will be awaited
                result = asyncio.ensure_future(maybe_async)
        return result

    return wrapped()


class SparkKernelBase(IPythonKernel):
    def __init__(
        self,
        implementation,
        implementation_version,
        language,
        language_version,
        language_info,
        session_language,
        user_code_parser=None,
        **kwargs
    ):
        # Required by Jupyter - Override
        self.implementation = implementation
        self.implementation_version = implementation_version
        self.language = language
        self.language_version = language_version
        self.language_info = language_info

        # Override
        self.session_language = session_language

        # NOTE: This is a (hopefully) temporary workaround to accommodate async do_execute in ipykernel>=6
        # Patch loop.run_until_complete as early as possible
        try:
            nest_asyncio.apply()
        except RuntimeError:
            # nest_asyncio requires a running loop in order to patch.
            # In tests the loop may not have been created yet.
            pass

        super().__init__(**kwargs)

        self.logger = SparkLog("{}_jupyter_kernel".format(self.session_language))
        self._fatal_error = None
        self.ipython_display = IpythonDisplay()

        if user_code_parser is None:
            self.user_code_parser = UserCodeParser()
        else:
            self.user_code_parser = user_code_parser

        # Disable warnings for test env in HDI
        requests.packages.urllib3.disable_warnings()

        # Do not load magics in testing
        if kwargs.get("testing", False):
            return

        # Load magics on init
        self._load_magics_extension()
        self._change_language()
        if conf.use_auto_viz():
            self._register_auto_viz()

    def do_execute(
        self, code, silent, store_history=True, user_expressions=None, allow_stdin=False
    ):
        def f(self):
            if self._fatal_error is not None:
                return self._repeat_fatal_error()

            return self._do_execute(
                code, silent, store_history, user_expressions, allow_stdin
            )

        # Execute the code and handle exceptions
        wrapped = wrap_unexpected_exceptions(f, self._complete_cell)
        return wrapped(self)

    def do_shutdown(self, restart):
        # Cleanup
        self._delete_session()

        return self._do_shutdown_ipykernel(restart)

    def _do_execute(self, code, silent, store_history, user_expressions, allow_stdin):
        code_to_run = self.user_code_parser.get_code_to_run(code)

        res = self._execute_cell(
            code_to_run, silent, store_history, user_expressions, allow_stdin
        )

        return res

    def _load_magics_extension(self):
        register_magics_code = "%load_ext sparkmagic.kernels"
        self._execute_cell(
            register_magics_code,
            True,
            False,
            shutdown_if_error=True,
            log_if_error="Failed to load the Spark kernels magics library.",
        )
        self.logger.debug("Loaded magics.")

    def _change_language(self):
        register_magics_code = "%%_do_not_call_change_language -l {}\n ".format(
            self.session_language
        )
        self._execute_cell(
            register_magics_code,
            True,
            False,
            shutdown_if_error=True,
            log_if_error="Failed to change language to {}.".format(
                self.session_language
            ),
        )
        self.logger.debug("Changed language.")

    def _register_auto_viz(self):
        from sparkmagic.utils.sparkevents import get_spark_events_handler
        import autovizwidget.utils.configuration as c

        handler = get_spark_events_handler()
        c.override("events_handler", handler)

        register_auto_viz_code = """from autovizwidget.widget.utils import display_dataframe
ip = get_ipython()
ip.display_formatter.ipython_display_formatter.for_type_by_name('pandas.core.frame', 'DataFrame', display_dataframe)"""
        self._execute_cell(
            register_auto_viz_code,
            True,
            False,
            shutdown_if_error=True,
            log_if_error="Failed to register auto viz for notebook.",
        )
        self.logger.debug("Registered auto viz.")

    def _delete_session(self):
        code = "%%_do_not_call_delete_session\n "
        self._execute_cell_for_user(code, True, False)

    def _execute_cell(
        self,
        code,
        silent,
        store_history=True,
        user_expressions=None,
        allow_stdin=False,
        shutdown_if_error=False,
        log_if_error=None,
    ):
        reply_content = self._execute_cell_for_user(
            code, silent, store_history, user_expressions, allow_stdin
        )

        if shutdown_if_error and reply_content["status"] == "error":
            error_from_reply = reply_content["evalue"]
            if log_if_error is not None:
                message = '{}\nException details:\n\t"{}"'.format(
                    log_if_error, error_from_reply
                )
                return self._abort_with_fatal_error(message)

        return reply_content

    def _execute_cell_for_user(
        self, code, silent, store_history=True, user_expressions=None, allow_stdin=False
    ):
        result = super().do_execute(
            code, silent, store_history, user_expressions, allow_stdin
        )

        # In ipykernel 6, this returns native asyncio coroutine
        if asyncio.iscoroutine(result):
            return run_sync(result)

        # In ipykernel 5, this returns gen.coroutine
        if asyncio.isfuture(result):
            return result.result()

        # In ipykernel 4, this func is synchronous
        return result

    def _do_shutdown_ipykernel(self, restart):
        result = super().do_shutdown(restart)

        # In tests, super() calls this SparkKernelBase.do_shutdown, which is async
        if asyncio.iscoroutine(result):
            return run_sync(result)

        return result

    def _complete_cell(self):
        """A method that runs a cell with no effect.

        Call this and return the value it returns when there's some sort
        of error preventing the user's cell from executing; this will
        register the cell from the Jupyter UI as being completed.
        """
        return self._execute_cell("None", False, True, None, False)

    def _show_user_error(self, message):
        self.logger.error(message)
        self.ipython_display.send_error(message)

    def _queue_fatal_error(self, message):
        """Queues up a fatal error to be thrown when the next cell is executed;
        does not raise an error immediately.

        We use this for errors that happen on kernel startup, since
        IPython crashes if we throw an exception in the __init__ method.
        """
        self._fatal_error = message

    def _abort_with_fatal_error(self, message):
        """Queues up a fatal error and throws it immediately."""
        self._queue_fatal_error(message)
        return self._repeat_fatal_error()

    def _repeat_fatal_error(self):
        """Throws an error that has already been queued."""
        error = conf.fatal_error_suggestion().format(self._fatal_error)
        self.logger.error(error)
        self.ipython_display.send_error(error)
        return self._complete_cell()
