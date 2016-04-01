class LivyClientLibException(Exception):
    pass


class FailedToCreateSqlContextException(LivyClientLibException):
    """Exception that is thrown when the SQL context fails to be created on session start."""


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


class BadUserDataException(LivyClientLibException):
    """An exception that is thrown when data provided by the user is invalid
    in some way"""
