# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from plotly.graph_objs import Figure, Layout
from plotly.offline import iplot
try:
    from pandas.core.base import DataError
except:
    from pandas.core.groupby import DataError

from ..widget.encoding import Encoding
from ..widget.invalidencodingerror import InvalidEncodingError


class GraphBase(object):
    def render(self, df, encoding, output):
        if encoding.x is None or encoding.y is None:
            with output:
                print("\n\n\nPlease select an X and Y axis.")
                return

        try:
            data = self._get_data(df, encoding)
        except InvalidEncodingError as err:
            with output:
                print("\n\n\n{}".format(err))
                return

        type_x_axis = self._get_type_axis(encoding.logarithmic_x_axis)
        type_y_axis = self._get_type_axis(encoding.logarithmic_y_axis)

        layout = Layout(xaxis=dict(type=type_x_axis, rangemode="tozero", title=encoding.x),
                        yaxis=dict(type=type_y_axis, rangemode="tozero", title=encoding.y))

        with output:
            try:
                fig = Figure(data=data, layout=layout)
                iplot(fig, show_link=False)
            except TypeError:
                print("\n\n\nPlease select another set of X and Y axis, because the type of the current axis do\n"
                      "not support aggregation over it.")

    @staticmethod
    def display_x():
        return True

    @staticmethod
    def display_y():
        return True

    @staticmethod
    def display_logarithmic_x_axis():
        return True

    @staticmethod
    def display_logarithmic_y_axis():
        return True

    @staticmethod
    def _get_type_axis(boolean_value):
        if boolean_value:
            return "log"
        return "-"

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
        if y_aggregation == Encoding.y_agg_none:
            raise ValueError("No Y aggregation function specified.")

        # Pandas has some confusing behavior when it comes to aggregating
        # over empty dataframes. We're just going to explicitly block against that here.
        if len(df) == 0:
            raise InvalidEncodingError("Cannot display graph for an empty data set.")

        try:
            df_grouped = df.groupby(x_column)
        except TypeError:
            raise InvalidEncodingError("Cannot group by X column '{}' because of its type: '{}'."
                                       .format(df[x_column].dtype))
        else:
            try:
                if y_aggregation == Encoding.y_agg_avg:
                    df_transformed = df_grouped.mean()
                elif y_aggregation == Encoding.y_agg_min:
                    df_transformed = df_grouped.min()
                elif y_aggregation == Encoding.y_agg_max:
                    df_transformed = df_grouped.max()
                elif y_aggregation == Encoding.y_agg_sum:
                    df_transformed = df_grouped.sum()
                elif y_aggregation == Encoding.y_agg_count:
                    df_transformed = df_grouped.count()
                else:
                    raise ValueError("Y aggregation '{}' not supported.".format(y_aggregation))
            except (DataError, ValueError) as err:
                raise InvalidEncodingError("Cannot aggregate column '{}' with aggregation function '{}' because:\n\t'{}'."
                                           .format(y_column, y_aggregation, err))
            except TypeError:
                raise InvalidEncodingError("Cannot aggregate column '{}' with aggregation function '{}' because the type\n"
                                           "cannot be aggregated over."
                                           .format(y_column, y_aggregation))
            else:
                df_transformed = df_transformed.reset_index()

                if y_column not in df_transformed.columns:
                    raise InvalidEncodingError("Y column '{}' is not valid with aggregation function '{}'. Please select "
                                               "a different\naggregation function.".format(y_column, y_aggregation))

                x_values = df_transformed[x_column].tolist()
                y_values = df_transformed[y_column].tolist()

                return x_values, y_values
