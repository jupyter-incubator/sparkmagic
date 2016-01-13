# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd

from remotespark.datawidgets.ipythondisplay import IpythonDisplay


class DataGraph(object):
    """This does not use the table version of plotly because it freezes up the browser for 60 rows. Instead, we use
    pandas df HTML representation."""

    @staticmethod
    def render(df, encoding, output):
        with output:
            max_rows = pd.get_option("display.max_rows")
            max_cols = pd.get_option("display.max_columns")
            show_dimensions = pd.get_option("display.show_dimensions")

            # This will hide the index column for pandas df.
            IpythonDisplay.html_to_ipython("""
<style>
    table.dataframe.hideme thead th:first-child {
        display: none;
    }
    table.dataframe.hideme tbody th {
        display: none;
    }
</style>
""")
            IpythonDisplay.html_to_ipython(df.to_html(max_rows=max_rows, max_cols=max_cols,
                                                      show_dimensions=show_dimensions, notebook=True, classes="hideme"))

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
