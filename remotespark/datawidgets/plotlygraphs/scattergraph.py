from plotly.graph_objs import Scatter

from .graphbase import GraphBase


class ScatterGraph(GraphBase):

    def _get_data(self, df, encoding):
        x_values, y_values = GraphBase._get_x_y_values(df, encoding)
        return [Scatter(x=x_values, y=y_values, mode='markers')]
