# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from remotespark.utils.utils import get_connection_string, get_connection_string_elements


class AddEndpointWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(AddEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        widget_width = "800px"

        self.endpoints = endpoints
        self.endpoints_dropdown_widget = endpoints_dropdown_widget
        self.refresh_method = refresh_method

        self.address_widget = self.ipywidget_factory.get_text(
            description='Address:',
            value='http://example.com/livy',
            width=widget_width
        )
        self.user_widget = self.ipywidget_factory.get_text(
            description='Username:',
            value='username',
            width=widget_width
        )
        self.password_widget = self.ipywidget_factory.get_text(
            description='Password:',
            value='password',
            width=widget_width
        )

        self.conn_string_widget = self.ipywidget_factory.get_text(
            description='Connection String:',
            value=get_connection_string(self.address_widget.value, self.user_widget.value, self.password_widget.value),
            width=widget_width
        )

        # Sync values
        def parts_to_conn(name, old_value, new_value):
            url = self.address_widget.value
            user = self.user_widget.value
            password = self.password_widget.value
            conn_str = get_connection_string(url, user, password)
            self.conn_string_widget.value = conn_str

        def conn_to_parts(name, old_value, new_value):
            try:
                conn_str = self.conn_string_widget.value
                cso = get_connection_string_elements(conn_str)
                self.address_widget.value = cso.url
                self.user_widget.value = cso.username
                self.password_widget.value = cso.password
            except ValueError:
                pass

        self.address_widget.on_trait_change(parts_to_conn, "value")
        self.user_widget.on_trait_change(parts_to_conn, "value")
        self.password_widget.on_trait_change(parts_to_conn, "value")
        self.conn_string_widget.on_trait_change(conn_to_parts, "value")

        # Submit widget
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Add endpoint'
        )

        self.children = [self.ipywidget_factory.get_html(value="<br/>", width=widget_width), self.address_widget,
                         self.user_widget, self.password_widget, self.conn_string_widget,
                         self.ipywidget_factory.get_html(value="<br/>", width=widget_width), self.submit_widget]

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
