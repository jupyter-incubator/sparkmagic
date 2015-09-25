"""Utilities to construct or deconstruct connection strings for remote Spark submission of format:
       dnsname={CLUSTERDNSNAME};username={HTTPUSER};password={PASSWORD}
"""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from collections import namedtuple


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
