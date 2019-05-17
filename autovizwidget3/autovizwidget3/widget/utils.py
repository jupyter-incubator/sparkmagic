import pandas as pd

from .encoding import Encoding
from .autovizwidget import AutoVizWidget


def infer_vegalite_type(data):
    """
    From an array-like input, infer the correct vega typecode
    ('O', 'N', 'Q', or 'T')
    Parameters
    ----------
    data: Numpy array or Pandas Series
    """

    typ = pd.api.types.infer_dtype(data)

    if typ in ['floating', 'mixed-integer-float', 'integer',
               'mixed-integer', 'complex']:
        typecode = 'Q'
    elif typ in ['string', 'bytes', 'categorical', 'boolean', 'mixed', 'unicode']:
        typecode = 'N'
    elif typ in ['datetime', 'datetime64', 'timedelta',
                 'timedelta64', 'date', 'time', 'period']:
        typecode = 'T'
    else:
        typecode = 'N'

    return typecode


def _validate_custom_order(order):
    assert len(order) == 4
    list_to_check = list(order)
    list_to_check.sort()
    assert list_to_check == ['N', 'O', 'Q', 'T']


def _classify_data_by_type(data, order, skip=None):
    """Get O, N, Q, or T vegalite type for all columns in data except if in skip."""
    if skip is None:
        skip = []

    d = dict()
    for typ in order:
        d[typ] = []

    for column_name in data:
        if column_name not in skip:
            typ = infer_vegalite_type(data[column_name])
            d[typ].append(column_name)

    return d


def select_x(data, order=None):
    """
    Helper function that does a best effort of selecting an automatic x axis.
    Returns None if it cannot find x axis.
    """
    if data is None:
        return None

    if len(data) < 1:
        return None

    if order is None:
        order = ['T', 'O', 'N', 'Q']
    else:
        _validate_custom_order(order)

    d = _classify_data_by_type(data, order)

    chosen_x = None
    for typ in order:
        if len(d[typ]) >= 1:
            chosen_x = d[typ][0]
            break

    return chosen_x


def select_y(data, x_name, order=None, aggregator=None):
    """
    Helper function that does a best effort of selecting an automatic y axis.
    It won't set the same axis that x is set to again.
    Returns None if it cannot find y axis.
    """
    if data is None:
        return None

    if len(data) < 2:
        return None

    if x_name is None:
        return None

    if order is None:
        order = ['Q', 'O', 'N', 'T']
    else:
        _validate_custom_order(order)

    d = _classify_data_by_type(data, order, [x_name])

    # Choose the first column found on the following order: Q, O, N, T
    chosen_y = None
    for typ in order:
        if len(d[typ]) >= 1:
            chosen_y = d[typ][0]
            break

    return chosen_y


def display_dataframe(df):
    selected_x = select_x(df)
    selected_y = select_y(df, selected_x)
    encoding = Encoding(chart_type=Encoding.chart_type_table, x=selected_x, y=selected_y,
                        y_aggregation=Encoding.y_agg_max)
    return AutoVizWidget(df, encoding)
