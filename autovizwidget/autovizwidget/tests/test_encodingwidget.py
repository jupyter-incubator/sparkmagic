from mock import MagicMock, call
from nose.tools import with_setup
from ipywidgets import Widget
import pandas as pd

from ..widget.encodingwidget import EncodingWidget
from ..widget.encoding import Encoding


df = None
encoding = None
ipywidget_factory = None
change_hook = None


def _setup():
    global df, encoding, ipywidget_factory, change_hook

    records = [
        {"buildingID": 0, "date": "6/1/13", "temp_diff": 12, "\u263A": True},
        {"buildingID": 1, "date": "6/1/13", "temp_diff": 0, "\u263A": True},
        {"buildingID": 2, "date": "6/1/14", "temp_diff": 11, "\u263A": True},
        {"buildingID": 0, "date": "6/1/15", "temp_diff": 5, "\u263A": True},
        {"buildingID": 1, "date": "6/1/16", "temp_diff": 19, "\u263A": True},
        {"buildingID": 2, "date": "6/1/17", "temp_diff": 32, "\u263A": True},
    ]
    df = pd.DataFrame(records)

    encoding = Encoding(chart_type="table", x="date", y="temp_diff")

    ipywidget_factory = MagicMock()
    ipywidget_factory.get_vbox.return_value = MagicMock(spec=Widget)

    change_hook = MagicMock()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_encoding_with_all_none_doesnt_throw():
    records = [
        {"buildingID": 0, "date": "6/1/13", "temp_diff": 12},
        {"buildingID": 1, "date": "6/1/13", "temp_diff": 0},
        {"buildingID": 2, "date": "6/1/14", "temp_diff": 11},
        {"buildingID": 0, "date": "6/1/15", "temp_diff": 5},
        {"buildingID": 1, "date": "6/1/16", "temp_diff": 19},
        {"buildingID": 2, "date": "6/1/17", "temp_diff": 32},
    ]
    df = pd.DataFrame(records)

    encoding = Encoding()

    ipywidget_factory = MagicMock()
    ipywidget_factory.get_vbox.return_value = MagicMock(spec=Widget)

    EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    assert (
        call(
            description="X",
            value=None,
            options={
                "date": "date",
                "temp_diff": "temp_diff",
                "-": None,
                "buildingID": "buildingID",
            },
        )
        in ipywidget_factory.get_dropdown.mock_calls
    )
    assert (
        call(
            description="Y",
            value=None,
            options={
                "date": "date",
                "temp_diff": "temp_diff",
                "-": None,
                "buildingID": "buildingID",
            },
        )
        in ipywidget_factory.get_dropdown.mock_calls
    )
    assert (
        call(
            description="Func.",
            value="none",
            options={
                "Max": "Max",
                "Sum": "Sum",
                "Avg": "Avg",
                "-": "None",
                "Min": "Min",
                "Count": "Count",
            },
        )
        in ipywidget_factory.get_dropdown.mock_calls
    )


@with_setup(_setup, _teardown)
def test_value_for_aggregation():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    assert widget._get_value_for_aggregation(None) == "none"
    assert widget._get_value_for_aggregation("avg") == "avg"


@with_setup(_setup, _teardown)
def test_x_changed_callback():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._x_changed_callback("name", "old", "new")

    assert encoding.x == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_y_changed_callback():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._y_changed_callback("name", "old", "new")

    assert encoding.y == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_y_agg__changed_callback():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._y_agg_changed_callback("name", "old", "new")

    assert encoding.y_aggregation == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_log_x_changed_callback():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._logarithmic_x_callback("name", "old", "new")

    assert encoding.logarithmic_x_axis == "new"
    assert change_hook.call_count == 1


@with_setup(_setup, _teardown)
def test_log_y_changed_callback():
    widget = EncodingWidget(df, encoding, change_hook, ipywidget_factory, testing=True)

    widget._logarithmic_y_callback("name", "old", "new")

    assert encoding.logarithmic_y_axis == "new"
    assert change_hook.call_count == 1
