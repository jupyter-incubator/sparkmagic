# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Bar

from .graphbase import GraphBase


class BarGraph(GraphBase):
    def _get_data(self, df, encoding):
        x_values, y_values = GraphBase._get_x_y_values(df, encoding)
        return [Bar(x=x_values, y=y_values)]

