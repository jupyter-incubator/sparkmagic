from mock import MagicMock, call
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from ..widget.autovizwidget import AutoVizWidget
from ..widget.encoding import Encoding


renderer = None
df = None
encoding = None
encoding_widget = None
output = None
ipywidget_factory = None
ipython_display = None
spark_events = None


def setup_function():
    global renderer, df, encoding, encoding_widget, output, ipywidget_factory, ipython_display, spark_events

    renderer = MagicMock()
    renderer.display_x.return_value = True
    renderer.display_y.return_value = True
    renderer.display_controls.return_value = True
    renderer.display_logarithmic_x_axis.return_value = True
    renderer.display_logarithmic_y_axis.return_value = True

    records = [
        {"buildingID": 0, "date": "6/1/13", "temp_diff": 12},
        {"buildingID": 1, "date": "6/1/13", "temp_diff": 0},
        {"buildingID": 2, "date": "6/1/14", "temp_diff": 11},
        {"buildingID": 0, "date": "6/1/15", "temp_diff": 5},
        {"buildingID": 1, "date": "6/1/16", "temp_diff": 19},
        {"buildingID": 2, "date": "6/1/17", "temp_diff": 32},
    ]
    df = pd.DataFrame(records)

    encoding = Encoding(chart_type="table", x="date", y="temp_diff")

    ipywidget_factory = MagicMock()
    output = MagicMock()
    ipywidget_factory.get_output.return_value = output

    encoding_widget = MagicMock()
    ipython_display = MagicMock()

    spark_events = MagicMock()


def teardown_function():
    pass


def test_on_render_viz():
    widget = AutoVizWidget(
        df,
        encoding,
        renderer,
        ipywidget_factory,
        encoding_widget,
        ipython_display,
        spark_events=spark_events,
        testing=True,
    )

    # on_render_viz is called in the constructor, so no need to call it here.
    output.clear_output.assert_called_once_with()

    assert len(renderer.render.mock_calls) == 1
    assert len(renderer.render.mock_calls[0]) == 3
    assert renderer.render.mock_calls[0][1][1] == encoding
    assert renderer.render.mock_calls[0][1][2] == output
    assert_frame_equal(renderer.render.mock_calls[0][1][0], df)

    encoding_widget.show_x.assert_called_once_with(True)
    encoding_widget.show_y.assert_called_once_with(True)
    encoding_widget.show_controls.assert_called_once_with(True)
    encoding_widget.show_logarithmic_x_axis.assert_called_once_with(True)
    encoding_widget.show_logarithmic_y_axis.assert_called_once_with(True)

    spark_events.emit_graph_render_event.assert_called_once_with(encoding.chart_type)

    encoding._chart_type = Encoding.chart_type_scatter
    widget.on_render_viz()
    assert len(spark_events.emit_graph_render_event.mock_calls) == 2
    assert spark_events.emit_graph_render_event.call_args == call(
        Encoding.chart_type_scatter
    )


def test_create_viz_types_buttons():
    df_single_column = pd.DataFrame([{"buildingID": 0}])
    widget = AutoVizWidget(
        df_single_column,
        encoding,
        renderer,
        ipywidget_factory,
        encoding_widget,
        ipython_display,
        spark_events=spark_events,
        testing=True,
    )

    # create_viz_types_buttons is called in the constructor, so no need to call it here.
    assert (
        call(description=Encoding.chart_type_table)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_pie)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_line)
        not in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_area)
        not in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_bar)
        not in ipywidget_factory.get_button.mock_calls
    )
    spark_events.emit_graph_render_event.assert_called_once_with(encoding.chart_type)

    widget = AutoVizWidget(
        df,
        encoding,
        renderer,
        ipywidget_factory,
        encoding_widget,
        ipython_display,
        spark_events=spark_events,
        testing=True,
    )

    # create_viz_types_buttons is called in the constructor, so no need to call it here.
    assert (
        call(description=Encoding.chart_type_table)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_pie)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_line)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_area)
        in ipywidget_factory.get_button.mock_calls
    )
    assert (
        call(description=Encoding.chart_type_bar)
        in ipywidget_factory.get_button.mock_calls
    )


def test_create_viz_empty_df():
    df = pd.DataFrame([])
    widget = AutoVizWidget(
        df,
        encoding,
        renderer,
        ipywidget_factory,
        encoding_widget,
        ipython_display,
        spark_events=spark_events,
        testing=True,
    )

    ipywidget_factory.get_button.assert_not_called()
    ipywidget_factory.get_html.assert_called_once_with("No results.")
    ipython_display.display.assert_called_with(ipywidget_factory.get_html.return_value)
    spark_events.emit_graph_render_event.assert_called_once_with(encoding.chart_type)


def test_convert_to_displayable_dataframe():
    bool_df = pd.DataFrame(
        [
            {"bool_col": True, "int_col": 0, "float_col": 3.0},
            {"bool_col": False, "int_col": 100, "float_col": 0.7},
        ]
    )
    copy_of_df = bool_df.copy()
    widget = AutoVizWidget(
        df,
        encoding,
        renderer,
        ipywidget_factory,
        encoding_widget,
        ipython_display,
        spark_events=spark_events,
        testing=True,
    )
    result = AutoVizWidget._convert_to_displayable_dataframe(bool_df)
    # Ensure original DF not changed
    assert_frame_equal(bool_df, copy_of_df)
    assert_series_equal(bool_df["int_col"], result["int_col"])
    assert_series_equal(bool_df["float_col"], result["float_col"])
    assert result.dtypes["bool_col"] == object
    assert len(result["bool_col"]) == 2
    assert result["bool_col"][0] == "True"
    assert result["bool_col"][1] == "False"
    spark_events.emit_graph_render_event.assert_called_once_with(encoding.chart_type)
