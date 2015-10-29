# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd
import altair.api as alt
import sys

from .log import Log
from .configuration import get_configuration
from .constants import Constants


class AltairViewer(object):

    def __init__(self):
        self.logger = Log("AltairViewer")

    """A viewer that returns results as they are."""
    def visualize(self, result, chart_type=None):
        if type(result) is not pd.DataFrame:
            return result

        if chart_type is None:
            chart_type = get_configuration(Constants.default_chart_type, "area")

        columns = result.columns.values

        # Always return table for show tables
        if "isTemporary" in columns and "name" in columns:
            return result

        # Simply return dataframe if only 1 column is available
        if len(columns) <= 1 or chart_type == "table":
            return result

        # Create Altair Viz
        try:
            v = self.get_altair_viz(result)

            # Select default x
            v.select_x()

            # Select default y
            v.select_y(["Q", "O", "N", "T"], "avg")

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
        except:
            e = sys.exc_info()[0]
            self.logger.error("Unknown exception creating Altair viz. Exception {}".format(str(e)))
            return result

    def get_altair_viz(self, result):
        return alt.Viz(result)
