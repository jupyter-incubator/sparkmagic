# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import os
import uuid

from IPython.core.error import UsageError
from IPython.core.magic_arguments import parse_argstring
import numpy as np
import pandas as pd

from .filesystemreaderwriter import FileSystemReaderWriter

first_run = True
instance_id = None


def expand_path(path):
    return os.path.expanduser(path)


def join_paths(p1, p2):
    return os.path.join(p1, p2)


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
            except (ValueError, TypeError, OverflowError):
                pass

        if not coerced and df[column_name].dtype == np.dtype("object"):
            try:
                df[column_name] = pd.to_numeric(df[column_name], errors="raise")
                coerced = True
            except (ValueError, TypeError):
                pass
