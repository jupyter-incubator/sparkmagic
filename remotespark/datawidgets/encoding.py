# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class Encoding(object):
    def __init__(self, chart_type=None, x=None, y=None, y_aggregation=None):
        self._chart_type = chart_type
        self._x = x
        self._y = y
        self._y_aggregation = y_aggregation

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
