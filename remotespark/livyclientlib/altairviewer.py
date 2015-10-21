# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import pandas as pd
import altair.api as alt

class AltairViewer(object):
    """A viewer that returns results as they are."""
    def visualize(self, result):
        if type(result) == pd.DataFrame:
            columns = result.columns.values
            #v = alt.Viz(result).encode(x='buildingID', y='avg(temp_diff)')
            v = alt.Viz(result)
            # TODO: Can I inspect the type here to pick columns?!
            # TODO: Handle no data
            v.encode(x=columns[0], y=columns[1])
            # v.configure(width=6000, height=400, singleWidth=500, singleHeight=300)
            v.area()
            return v.render()
        else:
            return result
