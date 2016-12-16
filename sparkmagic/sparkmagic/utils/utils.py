# Distributed under the terms of the Modified BSD License.
from IPython.core.error import UsageError
from IPython.core.magic_arguments import parse_argstring
import numpy as np
import pandas as pd

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from .constants import LANG_SCALA, LANG_PYTHON, LANG_PYTHON3, LANG_R, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, SESSION_KIND_PYSPARK3


def get_livy_kind(language):
    if language == LANG_SCALA:
        return SESSION_KIND_SPARK
    elif language == LANG_PYTHON:
        return SESSION_KIND_PYSPARK
    elif language == LANG_PYTHON3:
        return SESSION_KIND_PYSPARK3
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
        raise BadUserDataException(str(e))
        
        
def coerce_pandas_df_to_numeric_datetime(df):
    for column_name in df.columns:
        coerced = False
        
        if df[column_name].isnull().all():
            continue

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

def get_sessions_info_html(info_sessions, current_session_id):
    html = u"""<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr>""" + \
    u"".join([session.get_row_html(current_session_id) for session in info_sessions]) + \
    u"</table>"

    return html
