# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from remotespark.controllerwidget.endpointwidget import EndpointWidget
from remotespark.controllerwidget.sessionwidget import SessionWidget
from remotespark.utils.constants import Constants


class MagicsControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display):
        super(MagicsControllerWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)

        self.endpoints = {Constants.default_endpoint: Constants.default_endpoint_conn_str}

        self._refresh()

    def run(self):
        pass

    def _refresh(self):
        self.endpoints_dropdown_widget = self.ipywidget_factory.get_dropdown(
                description="Endpoint:",
                options=self.endpoints,
                value=Constants.default_endpoint_conn_str
        )

        self.endpoint = EndpointWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                       self.endpoints, self.endpoints_dropdown_widget, self._refresh)
        self.add_session = SessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                         self.endpoints_dropdown_widget, self._refresh)

        self.tabs = self.ipywidget_factory.get_tab(children=[self.add_session, self.endpoint])
        self.tabs.set_title(0, "Manage Sessions")
        self.tabs.set_title(1, "Manage Endpoints")

        self.children = [self.tabs]

        for child in self.children:
            child.parent_widget = self
