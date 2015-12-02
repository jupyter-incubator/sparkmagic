# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd

from .log import Log
from .configuration import get_configuration
from .constants import Constants


class AutoVizViewer(object):

    def __init__(self):
        self.logger = Log("AutoVizViewer")

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

        return result
