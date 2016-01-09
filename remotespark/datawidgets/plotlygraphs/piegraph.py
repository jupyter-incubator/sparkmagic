# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Pie, Figure, Data
from plotly.offline import iplot


class PieGraph(object):
    @staticmethod
    def render(df, encoding, output):
        series = df.groupby([encoding.x]).size()
        data = [Pie(values=list(series.values), labels=series.index.tolist())]

        with output:
            fig = Figure(data=Data(data))
            iplot(fig, show_link=False)

    @staticmethod
    def display_logarithmic_x_axis():
        return False

    @staticmethod
    def display_logarithmic_y_axis():
        return False

    @staticmethod
    def display_x():
        return True

    @staticmethod
    def display_y():
        return False

    @staticmethod
    def _get_x_values(df, encoding):
        return df[encoding.x].tolist()
