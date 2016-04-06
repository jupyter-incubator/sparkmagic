"""Utilities to construct or deconstruct connection strings for remote Spark submission of format:
       dnsname={CLUSTERDNSNAME};username={HTTPUSER};password={PASSWORD}
"""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import os
import uuid

from IPython.core.error import UsageError
from IPython.core.magic_arguments import parse_argstring
import numpy as np
import pandas as pd

from .constants import LANG_SCALA, LANG_PYTHON, LANG_R, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK
from .filesystemreaderwriter import FileSystemReaderWriter

first_run = True
instance_id = None


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


def parse_argstring_or_throw(magic_func, argstring, parse_argstring=parse_argstring):
    """An alternative to the parse_argstring method from IPython.core.magic_arguments.
    Catches IPython.core.error.UsageError and propagates it as a
    livyclientlib.exceptions.BadUserDataException."""
    try:
        return parse_argstring(magic_func, argstring)
    except UsageError as e:
        from remotespark.livyclientlib.exceptions import BadUserDataException
        raise BadUserDataException(str(e))
