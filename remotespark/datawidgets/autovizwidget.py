# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import ipywidgets as w
from IPython.display import display


class AutoVizWidget(w.FlexBox):
    def __init__(self, df, encoding, renderer, **kwargs):
        assert encoding is not None
        assert df is not None
        assert type(df) is pd.DataFrame
        assert len(df.columns) > 0
        assert renderer is not None

        kwargs['orientation'] = 'vertical'
        super(AutoVizWidget, self).__init__((), **kwargs)

        self.df = df
        self.encoding = encoding
        self.renderer = renderer

        # Widget that will become the only child of AutoVizWidget
        self.widget = w.VBox()

        # Create output area
        self.to_display = w.Output()
        self.to_display.width = "800px"
        self.output = w.HBox()
        self.output.children = [self.to_display]

        # Create widget
        self.controls = self._create_left_pane()
        self.widget.children = [self.controls, self.output]
        self.children = [self.widget]

    def on_render_viz(self, *args):
        self.renderer.render(self.to_display, self.df, self.encoding)
        # self.controls.children

    def _create_left_pane(self):
        # Create types of viz hbox
        viz_types_hbox = self._create_viz_types_buttons()

        # Create vbox for shelves
        shelves_view = self._create_shelves_controls()

        # Create left pane with all controls
        left_pane = w.HBox()
        left_pane.children = [viz_types_hbox, shelves_view]

        return left_pane

    def _create_shelves_controls(self):
        vbox = w.VBox()
        children = list()
        children.append(w.HTML('Encoding:', width='148px', height='32px'))

        # X
        def x_changed_callback(name, old_value, new_value):
            self.encoding.x = new_value
            return self.on_render_viz()

        x_view = w.Dropdown(options={str(i): str(i) for i in self.df.columns},
                            description="X", value=self.df.columns[0])
        x_view.on_trait_change(x_changed_callback, 'value')
        children.append(x_view)

        # Y
        def y_changed_callback(name, old_value, new_value):
            self.encoding.y = new_value
            return self.on_render_viz()

        y_column_view = w.Dropdown(options={str(i): str(i) for i in self.df.columns},
                            description="Y", value=self.df.columns[1])
        y_column_view.on_trait_change(y_changed_callback, 'value')

        # Y aggregator
        def y_agg_changed_callback(name, old_value, new_value):
            self.encoding.y_aggregation = new_value
            return self.on_render_viz()

        y_agg_view = w.Dropdown(options={"-": "none", "Avg": "avg"}, description="Op.", value="none")
        y_agg_view.on_trait_change(y_agg_changed_callback, 'value')

        y_view = w.HBox()
        y_view.children = [y_column_view, y_agg_view]

        children.append(y_view)

        vbox.children = children
        return vbox

    def _create_viz_types_buttons(self):
        hbox = w.HBox()
        children = list()

        heading = w.HTML('Type:', width='50px', height='32px')
        children.append(heading)

        def on_render_pie(*args):
            self.encoding.chart_type = "pie"
            return self.on_render_viz()

        def on_render_area(*args):
            self.encoding.chart_type = "area"
            return self.on_render_viz()

        def on_render_line(*args):
            self.encoding.chart_type = "line"
            return self.on_render_viz()

        def on_render_bar(*args):
            self.encoding.chart_type = "bar"
            return self.on_render_viz()

        if len(self.df.columns) > 0:
            pie_button = w.Button(description='Pie')
            pie_button.on_click(on_render_pie)
            children.append(pie_button)

        if len(self.df.columns) > 1:
            area_button = w.Button(description='Area')
            area_button.on_click(on_render_area)
            children.append(area_button)

            line_button = w.Button(description='Line')
            line_button.on_click(on_render_line)
            children.append(line_button)

            bar_button = w.Button(description='Bar')
            bar_button.on_click(on_render_bar)
            children.append(bar_button)

        hbox.children = children

        return hbox
