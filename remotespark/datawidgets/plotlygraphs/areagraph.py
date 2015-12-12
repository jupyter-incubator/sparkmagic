# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Scatter

from graphbase import GraphBase


class AreaGraph(GraphBase):
    def _get_data(self, df, encoding):
        return [Scatter(x=GraphBase._get_x_values(df, encoding),
                        y=GraphBase._get_y_values(df, encoding),
                        fill="tonexty")]

    def display_x(self):
        return True

    def display_y(self):
        return True
