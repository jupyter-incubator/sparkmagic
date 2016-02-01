# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from remotespark.utils.utils import get_connection_string


class EndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(EndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.endpoints = endpoints
        self.endpoints_dropdown_widget = endpoints_dropdown_widget
        self.refresh_method = refresh_method

        self.address_widget = self.ipywidget_factory.get_text(
            description='Address:',
            value='http://example.com/livy',
        )
        self.user_widget = self.ipywidget_factory.get_text(
            description='Username:',
            value='username'
        )
        self.password_widget = self.ipywidget_factory.get_text(
            description='Password:',
            value='password'
        )
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Add endpoint'
        )

        self.children = [self.address_widget, self.user_widget, self.password_widget, self.submit_widget]

        for child in self.children:
            child.parent_widget = self

    def run(self):
        connection_string = get_connection_string(self.address_widget.value, self.user_widget.value,
                                                  self.password_widget.value)
        self.endpoints[self.address_widget.value] = connection_string

        self.ipython_display.writeln("Added endpoint {}".format(self.address_widget.value))

        # We need to call the refresh method because drop down in Tab 2 for endpoints wouldn't refresh with the new
        # value otherwise.
        self.refresh_method()
