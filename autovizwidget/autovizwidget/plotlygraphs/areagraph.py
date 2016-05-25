# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Scatter

from .graphbase import GraphBase


class AreaGraph(GraphBase):
    def _get_data(self, df, encoding):
        x_values, y_values = GraphBase._get_x_y_values(df, encoding)
        return [Scatter(x=x_values, y=y_values, fill="tonexty")]
