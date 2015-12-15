# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import ipywidgets as w
from IPython.display import display

from .plotlygraphs.graphrenderer import GraphRenderer
from .encodingwidget import EncodingWidget


class AutoVizWidget(w.FlexBox):
    def __init__(self, df, encoding, nested_widget_mode=False, **kwargs):
        assert encoding is not None
        assert df is not None
        assert type(df) is pd.DataFrame
        assert len(df.columns) > 0

        kwargs['orientation'] = 'vertical'
        super(AutoVizWidget, self).__init__((), **kwargs)

        self.df = df
        self.encoding = encoding
        self.renderer = GraphRenderer()

        # Widget that will become the only child of AutoVizWidget
        self.widget = w.VBox()

        # Create output area
        self.to_display = w.Output()
        self.to_display.width = "800px"
        self.output = w.HBox()
        self.output.children = [self.to_display]

        self.controls = self._create_controls_widget()

        if nested_widget_mode:
            self.widget.children = [self.controls, self.output]
            self.children = [self.widget]
        else:
            display(self.controls)
            display(self.to_display)

        self.on_render_viz()

    def on_render_viz(self, *args):
        # self.controls.children
        self.to_display.clear_output()

        self.renderer.render(self.df, self.encoding, self.to_display)

        self.encoding_widget.show_x(self.renderer.display_x(self.encoding.chart_type))
        self.encoding_widget.show_y(self.renderer.display_y(self.encoding.chart_type))
        self.encoding_widget.show_controls(self.renderer.display_controls(self.encoding.chart_type))
        self.encoding_widget.show_logarithmic_x_axis(self.renderer.display_logarithmic_x_axis(self.encoding.chart_type))
        self.encoding_widget.show_logarithmic_y_axis(self.renderer.display_logarithmic_y_axis(self.encoding.chart_type))

    def _create_controls_widget(self):
        # Create types of viz hbox
        viz_types_widget = self._create_viz_types_buttons()

        # Create encoding widget
        self.encoding_widget = EncodingWidget(self.df, self.encoding, self.on_render_viz)

        controls = w.VBox()
        controls.children = [viz_types_widget, self.encoding_widget]

        return controls

    def _create_viz_types_buttons(self):
        hbox = w.HBox()
        children = list()

        self.heading = w.HTML('Type:', width='80px', height='32px')
        children.append(self.heading)

        self._create_type_button("Table", children)
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
        button.padding = "10px"
        button.on_click(on_render)

        children.append(button)
