# Distributed under the terms of the Modified BSD License.
from IPython.core.error import UsageError
from IPython.core.magic_arguments import parse_argstring
import numpy as np
import pandas as pd
import json
from collections import OrderedDict

import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.livyclientlib.exceptions import BadUserDataException, DataFrameParseException


def get_coerce_value(coerce):
    if coerce is not None:
        coerce = coerce.lower() in ("yes", "true", "t", "y", "1")
    return coerce


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


def records_to_dataframe(records_text, kind, coerce=None):
    if records_text in ['', '[]']:
        strings = []
    else:
        strings = records_text.strip().split('\n')
    try:
        data_array = [json.JSONDecoder(object_pairs_hook=OrderedDict).decode(s) for s in strings]

        if kind == constants.SESSION_KIND_SPARKR and len(data_array) > 0:
            data_array = data_array[0]

        df = pd.DataFrame(data_array)

        if len(data_array) > 0:    
            # This will assign the columns in the right order. If we simply did
            # df = pd.DataFrame(data_array, columns=data_array[0].keys())
            # in the code defining df, above, we could get an issue where the first element
            # has some columns as null, and thus would drop the columns from the df altogether.
            # Refer to https://github.com/jupyter-incubator/sparkmagic/issues/346 for
            # more details.
            for data in data_array:
                if len(data.keys()) == len(df.columns):
                    df = df[list(data.keys())]
                    break
                    
        if coerce is None:
            coerce = conf.coerce_dataframe()
        if coerce:
            coerce_pandas_df_to_numeric_datetime(df)

        return df
    except ValueError:
        raise DataFrameParseException(u"Cannot parse object as JSON: '{}'".format(strings))


def get_sessions_info_html(info_sessions, current_session_id):
    html = u"""<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr>""" + \
    u"".join([session.get_row_html(current_session_id) for session in info_sessions]) + \
    u"</table>"

    return html
