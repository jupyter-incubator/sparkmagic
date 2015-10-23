# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd
import altair.api as alt

from .log import Log


class AltairViewer(object):

    logger = Log()

    """A viewer that returns results as they are."""
    def visualize(self, result, chart_type="area"):
        if type(result) is pd.DataFrame:
            columns = result.columns.values

            # Simply return dataframe if only 1 column is available
            if len(columns) <= 1 or chart_type == "table":
                return result

            # Create Altair Viz
            v = alt.Viz(result)

            # Select default x
            try:
                v.select_x()
            except AssertionError:
                self.logger.debug("Could not select x default value.")
                return result

            # Select default y
            try:
                v.select_y("avg")
            except AssertionError:
                self.logger.debug("Could not select y default value.")
                return result

            # Configure chart
            v.configure(width=800, height=400)

            # Select chart type
            if chart_type == "line":
                v.line()
            elif chart_type == "area":
                v.area()
            elif chart_type == "bar":
                v.bar()
            else:
                v.area()

            return v.render()
        else:
            return result

    @staticmethod
    def _get_x_shorthand(columns):
        return columns[0]

    @staticmethod
    def _get_y_shorthand(columns):
        return columns[1]
