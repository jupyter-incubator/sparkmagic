# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.tools import FigureFactory as FigFac
from plotly.offline import iplot


class DataGraph(object):
    @staticmethod
    def render(df, encoding, output):
        table = FigFac.create_table(df)

        with output:
            iplot(table, show_link=False)

    @staticmethod
    def display_logarithmic_x_axis():
        return False

    @staticmethod
    def display_logarithmic_y_axis():
        return False

    @staticmethod
    def display_x():
        return False

    @staticmethod
    def display_y():
        return False
