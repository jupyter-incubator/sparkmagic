# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Figure, Data
from plotly.offline import iplot


class GraphBase(object):
    def render(self, df, encoding, output):
        data = self._get_data(df, encoding)

        with output:
            fig = Figure(data=Data(data))
            iplot(fig, show_link=False)

    def display_x(self):
        raise NotImplementedError()

    def display_y(self):
        raise NotImplementedError()

    def _get_data(self, df, encoding):
        raise NotImplementedError()

    @staticmethod
    def _get_x_values(df, encoding):
        return df[encoding.x].tolist()

    @staticmethod
    def _get_y_values(df, encoding):
        return df[encoding.y].tolist()
