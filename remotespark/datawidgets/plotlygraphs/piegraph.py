# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Pie

from graphbase import GraphBase


class PieGraph(GraphBase):
    def _get_data(self, df, encoding):
        return [Pie(values=GraphBase._get_x_values(df, encoding))]

    def display_x(self):
        return True

    def display_y(self):
        return False
