# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Bar

from graphbase import GraphBase


class BarGraph(GraphBase):
    def _get_data(self, df, encoding):
        return [Bar(x=GraphBase._get_x_values(df, encoding), y=GraphBase._get_y_values(df, encoding))]

    def display_x(self):
        return True

    def display_y(self):
        return True
