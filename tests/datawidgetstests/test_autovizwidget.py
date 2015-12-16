from mock import MagicMock, call
from nose.tools import with_setup

from remotespark.datawidgets.autovizwidget import AutoVizWidgetTest
from remotespark.datawidgets.encoding import Encoding
import pandas as pd


renderer = None
df = None
encoding = None
encoding_widget = None
output = None
ipywidget_factory = None
ipython_display = None


def _setup():
    global renderer, df, encoding, encoding_widget, output, ipywidget_factory, ipython_display

    renderer = MagicMock()
    renderer.display_x.return_value = True
    renderer.display_y.return_value = True
    renderer.display_controls.return_value = True
    renderer.display_logarithmic_x_axis.return_value = True
    renderer.display_logarithmic_y_axis.return_value = True

    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0},
               {u'buildingID': 2, u'date': u'6/1/14', u'temp_diff': 11},
               {u'buildingID': 0, u'date': u'6/1/15', u'temp_diff': 5},
               {u'buildingID': 1, u'date': u'6/1/16', u'temp_diff': 19},
               {u'buildingID': 2, u'date': u'6/1/17', u'temp_diff': 32}]
    df = pd.DataFrame(records)

    encoding = Encoding(chart_type="table", x="date", y="temp_diff")

    ipywidget_factory = MagicMock()
    output = MagicMock()
    ipywidget_factory.get_output.return_value = output

    encoding_widget = MagicMock()
    ipython_display = MagicMock()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_on_render_viz():
    widget = AutoVizWidgetTest(df, encoding, renderer, ipywidget_factory,
                               encoding_widget, ipython_display, testing=True)

    # on_render_viz is called in the constructor, so no need to call it here.
    output.clear_output.assert_called_once()

    renderer.render.assert_called_once_with(df, encoding, output)

    encoding_widget.show_x.assert_called_once_with(True)
    encoding_widget.show_y.assert_called_once_with(True)
    encoding_widget.show_controls.assert_called_once_with(True)
    encoding_widget.show_logarithmic_x_axis.assert_called_once_with(True)
    encoding_widget.show_logarithmic_y_axis.assert_called_once_with(True)


@with_setup(_setup, _teardown)
def test_create_viz_types_buttons():
    df_single_column = pd.DataFrame([{u'buildingID': 0}])
    widget = AutoVizWidgetTest(df_single_column, encoding, renderer, ipywidget_factory,
                               encoding_widget, ipython_display, testing=True)

    # create_viz_types_buttons is called in the constructor, so no need to call it here.
    assert call(description=Encoding.chart_type_table) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_pie) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_line) not in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_area) not in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_bar) not in ipywidget_factory.get_button.mock_calls

    widget = AutoVizWidgetTest(df, encoding, renderer, ipywidget_factory,
                               encoding_widget, ipython_display, testing=True)

    # create_viz_types_buttons is called in the constructor, so no need to call it here.
    assert call(description=Encoding.chart_type_table) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_pie) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_line) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_area) in ipywidget_factory.get_button.mock_calls
    assert call(description=Encoding.chart_type_bar) in ipywidget_factory.get_button.mock_calls
