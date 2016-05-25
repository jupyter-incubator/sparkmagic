# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class Encoding(object):
    chart_type_scatter = "Scatter"
    chart_type_line = "Line"
    chart_type_area = "Area"
    chart_type_bar = "Bar"
    chart_type_pie = "Pie"
    chart_type_table = "Table"
    supported_chart_types = [chart_type_line, chart_type_area, chart_type_bar, chart_type_pie, chart_type_table]

    y_agg_avg = "Avg"
    y_agg_min = "Min"
    y_agg_max = "Max"
    y_agg_sum = "Sum"
    y_agg_none = "None"
    y_agg_count = "Count"
    supported_y_agg = [y_agg_avg, y_agg_min, y_agg_max, y_agg_sum, y_agg_none, y_agg_count]

    def __init__(self, chart_type=None, x=None, y=None, y_aggregation=None,
                 logarithmic_x_axis=False, logarithmic_y_axis=False):
        self._chart_type = chart_type
        self._x = x
        self._y = y
        self._y_aggregation = y_aggregation
        self._logarithmic_x_axis = logarithmic_x_axis
        self._logarithmic_y_axis = logarithmic_y_axis

    @property
    def chart_type(self):
        return self._chart_type

    @chart_type.setter
    def chart_type(self, value):
        self._chart_type = value

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, value):
        self._x = value

    @property
    def y(self):
        return self._y

    @y.setter
    def y(self, value):
        self._y = value

    @property
    def y_aggregation(self):
        return self._y_aggregation

    @y_aggregation.setter
    def y_aggregation(self, value):
        self._y_aggregation = value

    @property
    def logarithmic_x_axis(self):
        return self._logarithmic_x_axis

    @logarithmic_x_axis.setter
    def logarithmic_x_axis(self, value):
        self._logarithmic_x_axis = value

    @property
    def logarithmic_y_axis(self):
        return self._logarithmic_y_axis

    @logarithmic_y_axis.setter
    def logarithmic_y_axis(self, value):
        self._logarithmic_y_axis = value
