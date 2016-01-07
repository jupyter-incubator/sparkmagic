import pandas as pd

from remotespark.datawidgets.plotlygraphs.graphbase import GraphBase
from remotespark.datawidgets.plotlygraphs.piegraph import PieGraph
from remotespark.datawidgets.plotlygraphs.datagraph import DataGraph
from remotespark.datawidgets.encoding import Encoding
from remotespark.datawidgets.invalidencodingerror import InvalidEncodingError


def test_graph_base_display_methods():
    assert GraphBase.display_x()
    assert GraphBase.display_y()
    assert GraphBase.display_logarithmic_x_axis()
    assert GraphBase.display_logarithmic_y_axis()


def test_graphbase_get_x_y_values():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': 12},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': 0},
               {u'buildingID': 2, u'date': u'6/1/14', u'temp_diff': 11},
               {u'buildingID': 0, u'date': u'6/1/15', u'temp_diff': 5},
               {u'buildingID': 1, u'date': u'6/1/16', u'temp_diff': 19},
               {u'buildingID': 2, u'date': u'6/1/17', u'temp_diff': 32}]
    df = pd.DataFrame(records)
    expected_xs = [u'6/1/13', u'6/1/14', u'6/1/15', u'6/1/16', u'6/1/17']

    encoding = Encoding(chart_type=Encoding.chart_type_line, x="date", y="temp_diff", y_aggregation=Encoding.y_agg_sum)
    xs, yx = GraphBase._get_x_y_values(df, encoding)
    assert xs == expected_xs
    assert yx == [12, 11, 5, 19, 32]

    encoding = Encoding(chart_type=Encoding.chart_type_line, x="date", y="temp_diff", y_aggregation=Encoding.y_agg_avg)
    xs, yx = GraphBase._get_x_y_values(df, encoding)
    assert xs == expected_xs
    assert yx == [6, 11, 5, 19, 32]

    encoding = Encoding(chart_type=Encoding.chart_type_line, x="date", y="temp_diff", y_aggregation=Encoding.y_agg_max)
    xs, yx = GraphBase._get_x_y_values(df, encoding)
    assert xs == expected_xs
    assert yx == [12, 11, 5, 19, 32]

    encoding = Encoding(chart_type=Encoding.chart_type_line, x="date", y="temp_diff", y_aggregation=Encoding.y_agg_min)
    xs, yx = GraphBase._get_x_y_values(df, encoding)
    assert xs == expected_xs
    assert yx == [0, 11, 5, 19, 32]

    encoding = Encoding(chart_type=Encoding.chart_type_line, x="date", y="temp_diff", y_aggregation=Encoding.y_agg_none)
    xs, yx = GraphBase._get_x_y_values(df, encoding)
    assert xs == [u'6/1/13', u'6/1/13', u'6/1/14', u'6/1/15', u'6/1/16', u'6/1/17']
    assert yx == [12, 0, 11, 5, 19, 32]

    try:
        encoding = Encoding(chart_type=Encoding.chart_type_line, x="buildingID", y="date",
                            y_aggregation=Encoding.y_agg_avg)
        GraphBase._get_x_y_values(df, encoding)
        assert False
    except InvalidEncodingError:
        pass


def test_pie_graph_display_methods():
    assert PieGraph.display_x()
    assert not PieGraph.display_y()
    assert not PieGraph.display_logarithmic_x_axis()
    assert not PieGraph.display_logarithmic_y_axis()


def test_data_graph_display_methods():
    assert not DataGraph.display_x()
    assert not DataGraph.display_y()
    assert not DataGraph.display_logarithmic_x_axis()
    assert not DataGraph.display_logarithmic_y_axis()
