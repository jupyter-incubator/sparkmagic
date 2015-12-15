# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import pandas as pd
import ipywidgets as w


class EncodingWidget(w.FlexBox):
    def __init__(self, df, encoding, change_hook, **kwargs):
        assert encoding is not None
        assert df is not None
        assert type(df) is pd.DataFrame
        assert len(df.columns) > 0

        kwargs['orientation'] = 'vertical'
        super(EncodingWidget, self).__init__((), **kwargs)

        self.df = df
        self.encoding = encoding

        self.widget = w.VBox()

        self.title = w.HTML('Encoding:', width='148px', height='32px')

        # X view
        def x_changed_callback(name, old_value, new_value):
            self.encoding.x = new_value
            return change_hook()

        self.x_view = w.Dropdown(options={str(i): str(i) for i in self.df.columns},
                            description="X", value=self.encoding.x)
        self.x_view.on_trait_change(x_changed_callback, 'value')

        # Y
        def y_changed_callback(name, old_value, new_value):
            self.encoding.y = new_value
            return change_hook()

        y_column_view = w.Dropdown(options={str(i): str(i) for i in self.df.columns},
                            description="Y", value=self.encoding.y)
        y_column_view.on_trait_change(y_changed_callback, 'value')

        # Y aggregator
        def y_agg_changed_callback(name, old_value, new_value):
            if new_value == "none":
                self.encoding.y_aggregation = None
            else:
                self.encoding.y_aggregation = new_value
            return change_hook()

        value_for_view = "none"
        if encoding.y_aggregation is not None:
            value_for_view = encoding.y_aggregation
        y_agg_view = w.Dropdown(options={"-": "none", "Avg": "avg", "Min": "min", "Max": "max", "Sum": "sum"},
                                description="Func.",
                                value=value_for_view)
        y_agg_view.on_trait_change(y_agg_changed_callback, 'value')

        # Y view
        self.y_view = w.HBox()
        self.y_view.children = [y_column_view, y_agg_view]

        # Logarithmic X axis
        def logarithmic_x_callback(name, old_value, new_value):
            self.encoding.logarithmic_x_axis = new_value
            return change_hook()

        self.logarithmic_x_axis = w.Checkbox(description="Log scale X", value=encoding.logarithmic_x_axis)
        self.logarithmic_x_axis.on_trait_change(logarithmic_x_callback, "value")

        # Logarithmic Y axis
        def logarithmic_y_callback(name, old_value, new_value):
            self.encoding.logarithmic_y_axis = new_value
            return change_hook()

        self.logarithmic_y_axis = w.Checkbox(description="Log scale Y", value=encoding.logarithmic_y_axis)
        self.logarithmic_y_axis.on_trait_change(logarithmic_y_callback, "value")

        children = [self.title, self.x_view, self.y_view, self.logarithmic_x_axis, self.logarithmic_y_axis]
        self.widget.children = children

        self.children = [self.widget]

    def show_x(self, boolean):
        self.x_view.visible = boolean

    def show_logarithmic_x_axis(self, boolean):
        self.logarithmic_x_axis.visible = boolean

    def show_logarithmic_y_axis(self, boolean):
        self.logarithmic_y_axis.visible = boolean

    def show_y(self, boolean):
        self.y_view.visible = boolean

    def show_controls(self, boolean):
        self.widget.visible = boolean
