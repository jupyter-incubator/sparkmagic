# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.offline import init_notebook_mode

from .datagraph import DataGraph
from .piegraph import PieGraph
from .linegraph import LineGraph
from .areagraph import AreaGraph
from .bargraph import BarGraph


class GraphRenderer(object):

    def __init__(self):
        init_notebook_mode()

    @staticmethod
    def render(df, encoding, output):
        if encoding.chart_type == "line":
            LineGraph().render(df, encoding, output)
        elif encoding.chart_type == "area":
            AreaGraph().render(df, encoding, output)
        elif encoding.chart_type == "bar":
            BarGraph().render(df, encoding, output)
        elif encoding.chart_type == "pie":
            PieGraph().render(df, encoding, output)
        elif encoding.chart_type == "data":
            DataGraph().render(df, encoding, output)
        else:
            raise ValueError("Cannot display chart of type {}".format(encoding.chart_type))

    @staticmethod
    def display_x(encoding):
