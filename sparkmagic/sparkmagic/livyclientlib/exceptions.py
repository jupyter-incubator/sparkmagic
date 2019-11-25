import traceback
from sparkmagic.utils.constants import EXPECTED_ERROR_MSG, INTERNAL_ERROR_MSG


# == EXCEPTIONS ==
class LivyClientLibException(Exception):
    """Base class for all LivyClientLib exceptions. All exceptions that are explicitly raised by
    code in this package should be a subclass of LivyClientLibException. If you need to account for a
    new error condition, either use one of the existing LivyClientLibException subclasses,
    or create a new subclass with a descriptive name and add it to this file.

    We distinguish between "expected" errors, which represent errors that a user is likely
    to encounter in normal use, and "internal" errors, which represents exceptions that happen
    due to a bug in the library. Check EXPECTED_EXCEPTIONS to see which exceptions
    are considered "expected"."""


class HttpClientException(LivyClientLibException):
    """An exception thrown by the HTTP client when it fails to make a request."""


class LivyClientTimeoutException(LivyClientLibException):
    """An exception for timeouts while interacting with Livy."""

class DataFrameParseException(LivyClientLibException):
    """An internal error which suggests a bad implementation of dataframe parsing from JSON --
    if we get a JSON parsing error when parsing the results from the Livy server, this exception
    is thrown."""


class LivyUnexpectedStatusException(LivyClientLibException):
    """An exception that will be shown if some unexpected error happens on the Livy side."""


class SessionManagementException(LivyClientLibException):
    """An exception that is thrown by the Session Manager when it is a
    given session name is invalid in some way."""


class BadUserConfigurationException(LivyClientLibException):
    """An exception that is thrown when configuration provided by the user is invalid
    in some way."""


class BadUserDataException(LivyClientLibException):
    """An exception that is thrown when data provided by the user is invalid
    in some way."""


class SqlContextNotFoundException(LivyClientLibException):
    """Exception that is thrown when the SQL context is not found."""


class SparkStatementException(LivyClientLibException):
    """Exception that is thrown when an error occurs while parsing or executing Spark statements."""

# == DECORATORS FOR EXCEPTION HANDLING ==
EXPECTED_EXCEPTIONS = [BadUserConfigurationException, BadUserDataException, LivyUnexpectedStatusException, SqlContextNotFoundException, HttpClientException, LivyClientTimeoutException, SessionManagementException, SparkStatementException]

def handle_expected_exceptions(f):
    """A decorator that handles expected exceptions. Self can be any object with
    an "ipython_display" attribute.
    Usage:
    @handle_expected_exceptions
    def fn(self, ...):
        etc..."""
    from sparkmagic.utils import configuration as conf
    exceptions_to_handle = tuple(EXPECTED_EXCEPTIONS)

    # Notice that we're NOT handling e.DataFrameParseException here. That's because DataFrameParseException
    # is an internal error that suggests something is wrong with LivyClientLib's implementation.
    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except exceptions_to_handle as err:
            if conf.all_errors_are_fatal():
                raise err

            # Do not log! as some messages may contain private client information
            self.ipython_display.send_error(EXPECTED_ERROR_MSG.format(err))
            return None
        else:
            return out
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped


def wrap_unexpected_exceptions(f, execute_if_error=None):
    """A decorator that catches all exceptions from the function f and alerts the user about them.
    Self can be any object with a "logger" attribute and a "ipython_display" attribute.
    All exceptions are logged as "unexpected" exceptions, and a request is made to the user to file an issue
    at the Github repository. If there is an error, returns None if execute_if_error is None, or else
    returns the output of the function execute_if_error.
    Usage:
    @wrap_unexpected_exceptions
    def fn(self, ...):
        ..etc """
    from sparkmagic.utils import configuration as conf
    def handle_exception(self, e):
        self.logger.error(u"ENCOUNTERED AN INTERNAL ERROR: {}\n\tTraceback:\n{}".format(e, traceback.format_exc()))
        self.ipython_display.send_error(INTERNAL_ERROR_MSG.format(e))
        return None if execute_if_error is None else execute_if_error()

    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except Exception as err:
            if conf.all_errors_are_fatal():
                raise err
            return handle_exception(self, err)
        else:
            return out
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped
