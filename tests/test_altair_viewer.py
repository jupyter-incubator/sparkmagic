from mock import MagicMock
import pandas as pd
import numpy as np

from remotespark.livyclientlib.altairviewer import AltairViewer


def test_returns_the_same_when_not_pandas():
    result = MagicMock()
    viewer = AltairViewer()

    returned = viewer.visualize(result, "area")

    assert returned is result


def test_visualize_returns_pandas_when_empty():
    columns = ["buildingID", "date", "temp_diff"]
    result = pd.DataFrame.from_records(list(), columns=columns)

    viewer = AltairViewer()

    returned = viewer.visualize(result, "area")

    assert returned is result


def test_visualize_returns_pandas_when_table():
    result = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])

    viewer = AltairViewer()

    returned = viewer.visualize(result, "table")

    assert returned is result


def test_visualize_renders():
    result = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])

    viewer = AltairViewer()

    viz_m = MagicMock()
    viz_m.render.return_value = rendered = MagicMock()

    def mocked_get_altair_viz(ignored):
        return viz_m

    viewer.get_altair_viz = mocked_get_altair_viz

    # Area
    returned = viewer.visualize(result, "area")

    assert returned is rendered
    viz_m.select_x.assert_called_once_with()
    viz_m.select_y.assert_called_once_with("avg")
    viz_m.configure.assert_called_once_with(width=800, height=400)
    viz_m.area.assert_called_once_with()

    # Line
    viz_m.reset_mock()
    returned = viewer.visualize(result, "line")

    assert returned is rendered
    viz_m.select_x.assert_called_once_with()
    viz_m.select_y.assert_called_once_with("avg")
    viz_m.configure.assert_called_once_with(width=800, height=400)
    viz_m.line.assert_called_once_with()

    # Bar
    viz_m.reset_mock()
    returned = viewer.visualize(result, "bar")

    assert returned is rendered
    viz_m.select_x.assert_called_once_with()
    viz_m.select_y.assert_called_once_with("avg")
    viz_m.configure.assert_called_once_with(width=800, height=400)
    viz_m.bar.assert_called_once_with()
