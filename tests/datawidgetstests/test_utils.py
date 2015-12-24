from nose.tools import with_setup
import pandas as pd

import remotespark.datawidgets.utils as utils
from remotespark.datawidgets.encoding import Encoding


df = None
encoding = None


def _setup():
    global df, encoding

    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12, "mystr": "alejandro", "mystr2": "1"},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0, "mystr": "alejandro", "mystr2": "1"},
               {u'buildingID': 2, u'date': u'6/1/14', u'temp_diff': 11, "mystr": "alejandro", "mystr2": "1"},
               {u'buildingID': 0, u'date': u'6/1/15', u'temp_diff': 5, "mystr": "alejandro", "mystr2": "1.0"},
               {u'buildingID': 1, u'date': u'6/1/16', u'temp_diff': 19, "mystr": "alejandro", "mystr2": "1"},
               {u'buildingID': 2, u'date': u'6/1/17', u'temp_diff': 32, "mystr": "alejandro", "mystr2": "1"}]
    df = pd.DataFrame(records)

    encoding = Encoding(chart_type="table", x="date", y="temp_diff")


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_on_render_viz():
    utils.coerce_pandas_df_to_numeric_datetime(df)

    assert utils.infer_vegalite_type(df["buildingID"]) == "Q"
    assert utils.infer_vegalite_type(df["date"]) == "T"
    assert utils.infer_vegalite_type(df["temp_diff"]) == "Q"
    assert utils.infer_vegalite_type(df["mystr"]) == "N"
    assert utils.infer_vegalite_type(df["mystr2"]) == "Q"


def test_select_x():
    assert utils.select_x(None) is None

    def _check(d, expected):
        x = utils.select_x(d)
        assert x == expected

    data = dict(col1=[1.0, 2.0, 3.0],  # Q
                col2=['A', 'B', 'C'],  # N
                col3=pd.date_range('2012', periods=3, freq='A'))  # T
    _check(data, 'col3')

    data = dict(col1=[1.0, 2.0, 3.0],  # Q
                col2=['A', 'B', 'C'])  # N
    _check(data, 'col2')

    data = dict(col1=[1.0, 2.0, 3.0])  # Q
    _check(data, 'col1')

    # Custom order
    data = dict(col1=[1.0, 2.0, 3.0],  # Q
                col2=['A', 'B', 'C'],  # N
                col3=pd.date_range('2012', periods=3, freq='A'),  # T
                col4=pd.date_range('2012', periods=3, freq='A'))  # T
    selected_x = utils.select_x(data, ['N', 'T', 'Q', 'O'])
    assert selected_x == "col2"

    # Len < 1
    assert utils.select_x(dict()) is None


def test_select_y():
    def _check(d, expected):
        x = 'col1'
        y = utils.select_y(d, x)
        assert y == expected

    data = dict(col1=[1.0, 2.0, 3.0],  # Chosen X
                col2=['A', 'B', 'C'],  # N
                col3=pd.date_range('2012', periods=3, freq='A'),  # T
                col4=pd.date_range('2012', periods=3, freq='A'),  # T
                col5=[1.0, 2.0, 3.0])  # Q
    _check(data, 'col5')

    data = dict(col1=[1.0, 2.0, 3.0],  # Chosen X
                col2=['A', 'B', 'C'],  # N
                col3=pd.date_range('2012', periods=3, freq='A'))  # T
    _check(data, 'col2')

    data = dict(col1=[1.0, 2.0, 3.0],  # Chosen X
                col2=pd.date_range('2012', periods=3, freq='A'))  # T
    _check(data, 'col2')

    # No data
    assert utils.select_y(None, "something") is None

    # Len < 2
    assert utils.select_y(dict(col1=[1.0, 2.0, 3.0]), "something") is None

    # No x
    assert utils.select_y(df, None) is None

    # Custom order
    data = dict(col1=[1.0, 2.0, 3.0],  # Chosen X
                col2=['A', 'B', 'C'],  # N
                col3=pd.date_range('2012', periods=3, freq='A'),  # T
                col4=pd.date_range('2012', periods=3, freq='A'),  # T
                col5=[1.0, 2.0, 3.0],  # Q
                col6=[1.0, 2.0, 3.0])  # Q
    selected_x = 'col1'
    selected_y = utils.select_y(data, selected_x, ['N', 'T', 'Q', 'O'])
    assert selected_y == 'col2'
