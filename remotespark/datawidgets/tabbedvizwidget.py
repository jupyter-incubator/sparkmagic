# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import ipywidgets as w
from plotly.offline import init_notebook_mode
# from IPython.display import display

from .plotlygraphs.graphrenderer import GraphRenderer
from .encodingwidget import EncodingWidget


class TabbedVizWidget(w.FlexBox):
    def __init__(self, df, encoding, **kwargs):
        assert encoding is not None
        assert df is not None
        assert type(df) is pd.DataFrame
        assert len(df.columns) > 0

        kwargs['orientation'] = 'vertical'
        super(TabbedVizWidget, self).__init__((), **kwargs)

        self.df = df
        self.encoding = encoding

        # Widget that will become the only child of AutoVizWidget
        self.widget = w.VBox()

        # Create output area
        self.to_display = w.Output()
        self.to_display.width = "800px"
        self.output = w.HBox()
        self.output.children = [self.to_display]

        self.renderer = GraphRenderer()

        # Create widget
        self.controls = self._create_controls()
        self.widget.children = [self.controls, self.output]
        self.children = [self.widget]

    def on_render_viz(self, *args):
        # self.controls.children
        self.to_display.clear_output()

        self.renderer.render(self.df, self.encoding, self.to_display)

    def _create_controls(self):
        # Create types of viz hbox
        viz_types_widget = self._create_viz_types_buttons()

        # Create vbox for shelves
        encoding_widget = EncodingWidget(self.df, self.encoding, self.on_render_viz)

        # Create left pane with all controls
        controls = w.VBox()
        controls.children = [viz_types_widget, encoding_widget]

        return controls

    def _create_viz_types_buttons(self):
        hbox = w.HBox()
        children = list()

        heading = w.HTML('Type:', width='50px', height='32px')
        children.append(heading)

        self._create_type_button("Data", children)
        self._create_type_button("Pie", children)

        if len(self.df.columns) > 1:
            self._create_type_button("Line", children)
            self._create_type_button("Area", children)
            self._create_type_button("Bar", children)

        hbox.children = children

        return hbox

    def _create_type_button(self, name, children):
        def on_render(*args):
            self.encoding.chart_type = name.lower()
            return self.on_render_viz()

        button = w.Button(description=name)
        button.on_click(on_render)

        children.append(button)
