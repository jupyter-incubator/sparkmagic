from mock import MagicMock
from nose.tools import with_setup

from remotespark.datawidgets.encodingwidget import EncodingWidgetTest
from remotespark.datawidgets.encoding import Encoding
import pandas as pd


df = None
encoding = None
ipywidget_factory = None
change_hook = None


def _setup():
    global df, encoding, ipywidget_factory, change_hook

    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0},
               {u'buildingID': 2, u'date': u'6/1/14', u'temp_diff': 11},
               {u'buildingID': 0, u'date': u'6/1/15', u'temp_diff': 5},
               {u'buildingID': 1, u'date': u'6/1/16', u'temp_diff': 19},
               {u'buildingID': 2, u'date': u'6/1/17', u'temp_diff': 32}]
    df = pd.DataFrame(records)

    encoding = Encoding(chart_type="table", x="date", y="temp_diff")

    ipywidget_factory = MagicMock()

    change_hook = MagicMock()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_value_for_aggregation():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    assert widget._get_value_for_aggregation(None) == "none"
    assert widget._get_value_for_aggregation("avg") == "avg"


@with_setup(_setup, _teardown)
def test_x_changed_callback():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._x_changed_callback("name", "old", "new")

    assert encoding.x == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_y_changed_callback():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._y_changed_callback("name", "old", "new")

    assert encoding.y == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_y_agg__changed_callback():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._y_agg_changed_callback("name", "old", "new")

    assert encoding.y_aggregation == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_log_x_changed_callback():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._logarithmic_x_callback("name", "old", "new")

    assert encoding.logarithmic_x_axis == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_log_y_changed_callback():
    widget = EncodingWidgetTest(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._logarithmic_y_callback("name", "old", "new")

    assert encoding.logarithmic_y_axis == "new"
    assert change_hook.call_count == 1
