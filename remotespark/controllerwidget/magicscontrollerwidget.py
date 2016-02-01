# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json
from ipywidgets import Box

from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.ipywidgetfactory import IpyWidgetFactory
from remotespark.utils.utils import get_connection_string
from remotespark.utils.constants import Constants
import remotespark.utils.configuration as conf


default_endpoint = " Add and select an endpoint "
default_endpoint_conn_str = ""


class AbstractMenuWidget(Box):
    def __init__(self, spark_controller, ipywidget_factory=None, ipython_display=None,
                 nested_widget_mode=False, testing=False, **kwargs):
        kwargs['orientation'] = 'vertical'

        if not testing:
            super(AbstractMenuWidget, self).__init__((), **kwargs)

        self.spark_controller = spark_controller

        if ipywidget_factory is None:
            ipywidget_factory = IpyWidgetFactory()
        self.ipywidget_factory = ipywidget_factory

        if ipython_display is None:
            ipython_display = IpythonDisplay()
        self.ipython_display = ipython_display

        self.children = []

        if not nested_widget_mode:
            self._repr_html_()

    def _repr_html_(self):
        for child in self.children:
            self.ipython_display.display(child)
        return ""

    def hide_all(self):
        for child in self.children:
            child.visible = False

    def run(self):
        raise NotImplementedError("Concrete menu widget must define run")


class MagicsControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display):
        super(MagicsControllerWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)

        self.endpoints = {default_endpoint: default_endpoint_conn_str}

        self._refresh()

    def run(self):
        pass

    def _refresh(self):
        self.endpoints_dropdown_widget = self.ipywidget_factory.get_dropdown(
                description="Endpoint:",
                options=self.endpoints,
                value=default_endpoint_conn_str
        )

        self.endpoint = EndpointWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                       self.endpoints, self.endpoints_dropdown_widget, self._refresh)
        self.add_session = SessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                         self.endpoints_dropdown_widget)

        self.tabs = self.ipywidget_factory.get_tab(children=[self.endpoint,
                                                             self.add_session])
        self.tabs.set_title(0, "Manage Endpoints")
        self.tabs.set_title(1, "Manage Sessions")

        self.children = [self.tabs]

        for child in self.children:
            child.parent_widget = self


class SessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints_dropdown_widget):
        # This is nested
        super(SessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.endpoints_dropdown_widget = endpoints_dropdown_widget

        self.session_widget = self.ipywidget_factory.get_text(
            description='Name:',
            value='session-name'
        )
        self.lang_widget = self.ipywidget_factory.get_toggle_buttons(
            description='Language:',
            options=[Constants.lang_scala, Constants.lang_python],
        )
        self.properties = self.ipywidget_factory.get_text(
            description='Properties:',
            value="{}"
        )
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Create Session'
        )

        self.children = [self.endpoints_dropdown_widget, self.session_widget, self.lang_widget, self.properties,
                         self.submit_widget]

        for child in self.children:
            child.parent_widget = self

    def run(self):
        try:
            properties_json = self.properties.value
            if properties_json.strip() != "":
                conf.override(conf.session_configs.__name__, json.loads(self.properties.value))
        except ValueError as e:
            self.ipython_display.send_error("Session properties must be a valid JSON string. Error:\n{}".format(e))
            return

        connection_string = self.endpoints_dropdown_widget.value
        language = self.lang_widget.value
        alias = self.session_widget.value
        skip = False
        properties = conf.get_session_properties(language)

        if connection_string == default_endpoint_conn_str:
            self.ipython_display.send_error("Please select a valid endpoint first.")
            return

        try:
            self.spark_controller.add_session(alias, connection_string, skip, properties)
        except ValueError as e:
            self.ipython_display.send_error("""Could not add endpoint with
name:
    {}
properties:
    {}

due to error: '{}'""".format(alias, properties, e))
            return


class EndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget, refresh_method):
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

        # We need to call the refresh method because dropdown in Tab 2 for endpoints wouldn't refresh with the new value
        # otherwise.
        self.refresh_method()
