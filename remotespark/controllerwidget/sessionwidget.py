# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json

from remotespark.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from remotespark.utils import configuration as conf
from remotespark.utils.constants import Constants


class SessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints_dropdown_widget, refresh_method):
        # This is nested
        super(SessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.refresh_method = refresh_method

        children = self.get_existing_session_widgets()

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

        children = children + [self.endpoints_dropdown_widget, self.session_widget, self.lang_widget, self.properties,
                               self.submit_widget]
        self.children = children

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

        if connection_string == Constants.default_endpoint_conn_str:
            self.ipython_display.send_error("Please select a valid endpoint first.")
            return

        try:
            self.spark_controller.add_session(alias, connection_string, skip, properties)
        except ValueError as e:
            self.ipython_display.send_error("""Could not add session with
name:
    {}
properties:
    {}

due to error: '{}'""".format(alias, properties, e))
            return

        self.refresh_method()

    def get_existing_session_widgets(self):
        session_widgets = []

        client_dict = self.spark_controller.get_managed_clients()
        if len(client_dict) > 0:

            header = self.get_session_widget("Name", "Id", "Kind", "State", False)
            session_widgets.append(header)
            session_widgets.append(self.ipywidget_factory.get_html(value="<hr/>", width="600px"))

            for name, client in client_dict.items():
                session_widgets.append(self.get_session_widget(name, client.session_id, client.kind, client.status))

        return session_widgets

    def get_session_widget(self, name, id, kind, state, button=True):
        hbox = self.ipywidget_factory.get_hbox()

        name_w = self.ipywidget_factory.get_html(value=name, width="200px")
        id_w = self.ipywidget_factory.get_html(value=id, width="100px")
        kind_w = self.ipywidget_factory.get_html(value=kind, width="100px")
        state_w = self.ipywidget_factory.get_html(value=state, width="100px")

        if button:
            def delete_on_click(button):
                self.spark_controller.delete_session_by_name(name)
                self.refresh_method()

            delete_w = self.ipywidget_factory.get_button(description="Delete")
            delete_w.on_click(delete_on_click)
        else:
            delete_w = self.ipywidget_factory.get_html(value="", width="100px")

        hbox.children = [name_w, id_w, kind_w, state_w, delete_w]

        return hbox
