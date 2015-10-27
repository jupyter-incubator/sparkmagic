# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd
import altair.api as alt

from .log import Log


class AltairViewer(object):

    logger = Log()

    """A viewer that returns results as they are."""
    def visualize(self, result, chart_type="area"):
        if type(result) is not pd.DataFrame:
            return result

        columns = result.columns.values

        # Simply return dataframe if only 1 column is available
        if len(columns) <= 1 or chart_type == "table":
            return result

        # Create Altair Viz
        try:
            v = self.get_altair_viz(result)

            # Select default x
            v.select_x()

            # Select default y
            v.select_y("avg")

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
        except (AssertionError, ValueError) as e:
            self.logger.error("Could not create Altair viz. Exception {}".format(str(e)))
            return result

    def get_altair_viz(self, result):
        return alt.Viz(result)
