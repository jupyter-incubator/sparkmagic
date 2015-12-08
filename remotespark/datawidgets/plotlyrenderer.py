# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Data, Scatter, Bar, Pie, Figure
from plotly.offline import init_notebook_mode, iplot
from plotly.tools import FigureFactory as FF


class PlotlyRenderer(object):
    def __init__(self):
        init_notebook_mode()

    @staticmethod
    def render(output, df, encoding):
        x_values = df[encoding.x].tolist()
        y_values = df[encoding.y].tolist()

        if encoding.chart_type == "line":
            data = Data([Scatter(x=x_values, y=y_values)])
        elif encoding.chart_type == "area":
            data = Data([Scatter(x=x_values, y=y_values, fill='tonexty')])
        elif encoding.chart_type == "bar":
            data = Data([Bar(x=x_values, y=y_values)])
        elif encoding.chart_type == "pie":
            data = Data([Pie(values=x_values)])
        else:
            raise ValueError("Cannot display chart of type {}".format(encoding.chart_type))

        with output:
            output.clear_output()
            fig = Figure(data=data)
            iplot(fig, show_link=False)
