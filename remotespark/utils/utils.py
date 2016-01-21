"""Utilities to construct or deconstruct connection strings for remote Spark submission of format:
       dnsname={CLUSTERDNSNAME};username={HTTPUSER};password={PASSWORD}
"""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from collections import namedtuple
import os
import uuid
import pandas as pd
import numpy as np

from .filesystemreaderwriter import FileSystemReaderWriter


first_run = True
instance_id = None


def get_connection_string(url, username, password):
    """Build a connection string for remote Spark submission via http basic authorization.
       
    Parameters
    ----------

    url : string
        The endpoint to hit.
    username : string
        The username for basic authorization.
    password : string
        The password for basic authorization.
       
    Returns
    -------
       
    connection_string : string
        A valid connection string for remote Spark submission.
    """

    return "url={};username={};password={}"\
        .format(url, username, password)


def get_connection_string_elements(connection_string):
    """Get the elements of a valid connection string for remote Spark submission 
    via http basic authorization.
           
    Parameters
    ----------
       
    connection_string : string
        The connection string.
           
    Returns
    -------
     
    connection_string : namedtuple
        Contains url, username, and password members.

    """
    connectionstring = namedtuple("ConnectionString", ["url", "username", "password"])

    d = dict(item.split("=") for item in connection_string.split(";"))

    cs = connectionstring(d["url"], d["username"], d["password"])
       
    return cs


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
            except:
                pass

        if not coerced and df[column_name].dtype == np.dtype("object"):
            try:
                df[column_name] = pd.to_numeric(df[column_name], errors="raise")
                coerced = True
            except:
                pass
