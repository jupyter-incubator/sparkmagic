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
    def _get_x_y_values(df, encoding):
        try:
            x_values, y_values = GraphBase._get_x_y_values_aggregated(df,
                                                                      encoding.x,
                                                                      encoding.y,
                                                                      encoding.y_aggregation)
        except ValueError:
            x_values = GraphBase._get_x_values(df, encoding)
            y_values = GraphBase._get_y_values(df, encoding)

        return x_values, y_values

    @staticmethod
    def _get_x_values(df, encoding):
        return df[encoding.x].tolist()

    @staticmethod
    def _get_y_values(df, encoding):
        return df[encoding.y].tolist()

    @staticmethod
    def _get_x_y_values_aggregated(df, x_column, y_column, y_aggregation):
        if y_aggregation is None:
            raise ValueError("No Y aggregation function specified.")

        df_grouped = df.groupby(x_column)

        if y_aggregation == "avg":
            df_transformed = df_grouped.mean()
        elif y_aggregation == "min":
            df_transformed = df_grouped.min()
        elif y_aggregation == "max":
            df_transformed = df_grouped.max()
        elif y_aggregation == "sum":
            df_transformed = df_grouped.sum()
        else:
            raise ValueError("Y aggregation '{}' not supported.".format(y_aggregation))

        df_transformed = df_transformed.reset_index()

        x_values = df_transformed[x_column].tolist()
        y_values = df_transformed[y_column].tolist()

        return x_values, y_values
