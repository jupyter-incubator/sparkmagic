# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd
import altair.api as alt


class AltairViewer(object):
    """A viewer that returns results as they are."""
    def visualize(self, result):
        if type(result) == pd.DataFrame:
            columns = result.columns.values

            # Simply return dataframe if only 1 column is available
            if len(columns) <= 1:
                return result

            # Create Altair Viz
            v = alt.Viz(result)

            # TODO: Add helper method to Altair to know column types? Can i get them from pandas df?
            # TODO: Select defaults - Do this on altair!: v.encode_default()
            x_shorthand = self._get_x_shorthand(columns)
            y_shorthand = self._get_y_shorthand(columns)

            # Ideal for example would be v = alt.Viz(result).encode(x='buildingID', y='avg(temp_diff)')
            v.encode(x=x_shorthand, y=y_shorthand)

            # Configure chart
            # v.configure(width=6000, height=400, singleWidth=500, singleHeight=300)
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
