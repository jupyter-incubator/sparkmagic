# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.offline import init_notebook_mode
import plotly as p

from .datagraph import DataGraph
from .piegraph import PieGraph
from .linegraph import LineGraph
from .areagraph import AreaGraph
from .bargraph import BarGraph
from .scattergraph import ScatterGraph
from ..widget.encoding import Encoding


class GraphRenderer(object):

    @staticmethod
    def render(df, encoding, output):
        with output:
            init_notebook_mode()

        GraphRenderer._get_graph(encoding.chart_type).render(df, encoding, output)

    @staticmethod
    def display_x(chart_type):
        return GraphRenderer._get_graph(chart_type).display_x()

    @staticmethod
    def display_y(chart_type):
        return GraphRenderer._get_graph(chart_type).display_y()

    @staticmethod
    def display_logarithmic_x_axis(chart_type):
        return GraphRenderer._get_graph(chart_type).display_logarithmic_x_axis()

    @staticmethod
    def display_logarithmic_y_axis(chart_type):
        return GraphRenderer._get_graph(chart_type).display_logarithmic_y_axis()

    @staticmethod
    def display_controls(chart_type):
        display_x = GraphRenderer.display_x(chart_type)
        display_y = GraphRenderer.display_y(chart_type)
        return display_x or display_y

    @staticmethod
    def _get_graph(chart_type):
        if chart_type == Encoding.chart_type_scatter:
            graph = ScatterGraph()
        elif chart_type == Encoding.chart_type_line:
            graph = LineGraph()
        elif chart_type == Encoding.chart_type_area:
            graph = AreaGraph()
        elif chart_type == Encoding.chart_type_bar:
            graph = BarGraph()
        elif chart_type == Encoding.chart_type_pie:
            graph = PieGraph()
        elif chart_type == Encoding.chart_type_table:
            graph = DataGraph()
        else:
            raise ValueError("Cannot display chart of type {}".format(chart_type))

        return graph
