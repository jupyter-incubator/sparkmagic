# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from ipywidgets import VBox, Output, Button, HTML, HBox


class IpyWidgetFactory(object):
    """This class exists solely for unit testing purposes."""

    @staticmethod
    def get_vbox(**kwargs):
        return VBox(**kwargs)

    @staticmethod
    def get_output(**kwargs):
        return Output(**kwargs)

    @staticmethod
    def get_button(**kwargs):
        return Button(**kwargs)

    @staticmethod
    def get_html(**kwargs):
        return HTML(**kwargs)

    @staticmethod
    def get_hbox(**kwargs):
        return HBox(**kwargs)
