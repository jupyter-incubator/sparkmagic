# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from ipywidgets import VBox, Output, Button, HTML, HBox, Dropdown, Checkbox, ToggleButtons, Text, Textarea, Tab


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
    def get_html(value, **kwargs):
        return HTML(value, **kwargs)

    @staticmethod
    def get_hbox(**kwargs):
        return HBox(**kwargs)

    @staticmethod
    def get_dropdown(**kwargs):
        return Dropdown(**kwargs)

    @staticmethod
    def get_checkbox(**kwargs):
        return Checkbox(**kwargs)

    @staticmethod
    def get_toggle_buttons(**kwargs):
        return ToggleButtons(**kwargs)

    @staticmethod
    def get_text(**kwargs):
        return Text(**kwargs)

    @staticmethod
    def get_text_area(**kwargs):
        return Textarea(**kwargs)

    @staticmethod
    def get_submit_button(**kwargs):
        return SubmitButton(**kwargs)

    @staticmethod
    def get_tab(**kwargs):
        return Tab(**kwargs)


class SubmitButton(Button):
    def __init__(self, **kwargs):
        super(SubmitButton, self).__init__(**kwargs)
        self.on_click(self.submit_clicked)

    def submit_clicked(self, button):
        self.parent_widget.run()
