# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from remotespark.controllerwidget.abstractmenuwidget import AbstractMenuWidget


class ManageEndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, refresh_method):
        # This is nested
        super(ManageEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.endpoints = endpoints
        self.refresh_method = refresh_method

        self.children = self.get_existing_endpoint_widgets()

        for child in self.children:
            child.parent_widget = self

    def run(self):
        self.refresh_method()

    def get_existing_endpoint_widgets(self):
        endpoint_widgets = []
        endpoint_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))

        if len(self.endpoints) > 0:
            # Header
            header = self.get_endpoint_widget("Endpoint", False)
            endpoint_widgets.append(header)
            endpoint_widgets.append(self.ipywidget_factory.get_html(value="<hr/>", width="600px"))

            # Endpoints
            for url, conn_str in self.endpoints.items():
                endpoint_widgets.append(self.get_endpoint_widget(url))

            endpoint_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            endpoint_widgets.append(self.ipywidget_factory.get_html(value="No endpoints yet.", width="600px"))

        return endpoint_widgets

    def get_endpoint_widget(self, url, button=True):
        hbox = self.ipywidget_factory.get_hbox()

        url_w = self.ipywidget_factory.get_html(value=url, width="500px", padding="4px")

        if button:
            def delete_on_click(button):
                self.endpoints.pop(url, None)
                self.refresh_method()

            delete_w = self.ipywidget_factory.get_button(description="Remove")
            delete_w.on_click(delete_on_click)
        else:
            delete_w = self.ipywidget_factory.get_html(value="", width="100px", padding="4px")

        hbox.children = [url_w, delete_w]

        return hbox
