"""Utilities to construct or deconstruct connection strings for remote Spark submission of format:
       dnsname={CLUSTERDNSNAME};username={HTTPUSER};password={PASSWORD}
"""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import os
import uuid
import pandas as pd
import numpy as np
import traceback

from .filesystemreaderwriter import FileSystemReaderWriter
from .constants import LANG_SCALA, LANG_PYTHON, LANG_R, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, INTERNAL_ERROR_MSG, \
    EXPECTED_ERROR_MSG


first_run = True
instance_id = None


def handle_expected_exceptions(f):
    """A decorator that handles expected exceptions. Self can be any object with
    an "ipython_display" attribute.
    Usage:
    @handle_expected_exceptions
    def fn(self, ...):
        etc..."""
    import remotespark.livyclientlib.exceptions as e
    exceptions_to_handle = (e.BadUserDataException, e.LivyUnexpectedStatusException, e.FailedToCreateSqlContextException,
                            e.HttpClientException, e.LivyClientTimeoutException, e.SessionManagementException)

    # Notice that we're NOT handling e.DataFrameParseException here. That's because DataFrameParseException
    # is an internal error that suggests something is wrong with LivyClientLib's implementation.
    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except exceptions_to_handle as err:
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
    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except Exception as e:
            self.logger.error("ENCOUNTERED AN INTERNAL ERROR: {}\n\tTraceback:\n{}".format(e, traceback.format_exc()))
            self.ipython_display.send_error(INTERNAL_ERROR_MSG.format(e))
            return None if execute_if_error is None else execute_if_error()
        else:
            return out
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped


def expand_path(path):
    return os.path.expanduser(path)


def join_paths(p1, p2):
    return os.path.join(p1, p2)


def get_magics_home_path():
    path = expand_path("~/.sparkmagic/")
    p = FileSystemReaderWriter(path)
    p.ensure_path_exists()
    return path


def generate_uuid():
    return uuid.uuid4()


def get_instance_id():
    global first_run, instance_id

    if first_run:
        first_run = False
        instance_id = generate_uuid()

    if instance_id is None:
        raise ValueError("Tried to return empty instance ID.")

    return instance_id


def coerce_pandas_df_to_numeric_datetime(df):
    for column_name in df.columns:
        coerced = False

        if not coerced and df[column_name].dtype == np.dtype("object"):
            try:
                df[column_name] = pd.to_datetime(df[column_name], errors="raise")
                coerced = True
            except (ValueError, TypeError):
                pass

        if not coerced and df[column_name].dtype == np.dtype("object"):
            try:
                df[column_name] = pd.to_numeric(df[column_name], errors="raise")
                coerced = True
            except (ValueError, TypeError):
                pass


def get_livy_kind(language):
    if language == LANG_SCALA:
        return SESSION_KIND_SPARK
    elif language == LANG_PYTHON:
        return SESSION_KIND_PYSPARK
    elif language == LANG_R:
        return SESSION_KIND_SPARKR
    else:
        raise ValueError("Cannot get session kind for {}.".format(language))

