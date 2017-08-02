# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from sparkmagic.livyclientlib.exceptions import HttpClientException
from sparkmagic.utils.sparklogger import SparkLog


class ManageEndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, refresh_method):
        # This is nested
        super(ManageEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.logger = SparkLog("ManageEndpointWidget")
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
            header = self.ipywidget_factory.get_html(value="Endpoint")
            endpoint_widgets.append(header)

            # Endpoints
            for url, endpoint in self.endpoints.items():
                try:
                    endpoint_widgets.append(self.get_endpoint_widget(url, endpoint))
                except HttpClientException:
                    # If we can't reach one of the default endpoints, just skip over it
                    if not endpoint.implicitly_added:
                        raise
                    else:
                        self.logger.info("Failed to connect to implicitly-defined endpoint at: %s" % url)

            endpoint_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            endpoint_widgets.append(self.ipywidget_factory.get_html(value="No endpoints yet.", width="600px"))

        return endpoint_widgets

    def get_endpoint_widget(self, url, endpoint):
        # 600 px
        width = "600px"
        vbox_outter = self.ipywidget_factory.get_vbox()
        separator = self.ipywidget_factory.get_html(value="<hr/>", width=width)

        hbox_outter = self.ipywidget_factory.get_hbox()
        hbox_outter_children = []
        try:
            vbox_left = self.get_endpoint_left(endpoint, url)
            cleanup_w = self.get_cleanup_button_endpoint(url, endpoint)

            hbox_outter_children.append(vbox_left)
            hbox_outter_children.append(cleanup_w)
        except ValueError as e:
            hbox_outter_children.append(self.ipywidget_factory.get_html(value=str(e), width=width))

        hbox_outter_children.append(self.get_delete_button_endpoint(url, endpoint))
        hbox_outter.children = hbox_outter_children

        vbox_outter.children = [separator, hbox_outter]

        return vbox_outter

    def get_endpoint_left(self, endpoint, url):
        # 400 px
        info = self.get_info_endpoint_widget(endpoint, url)
        delete_session_number = self.get_delete_session_endpoint_widget(url, endpoint)
        vbox_left = self.ipywidget_factory.get_vbox(children=[info, delete_session_number], width="400px")
        return vbox_left

    def get_cleanup_button_endpoint(self, url, endpoint):
        def cleanup_on_click(button):
            try:
                self.spark_controller.cleanup_endpoint(endpoint)
            except ValueError as e:
                self.ipython_display.send_error("Could not clean up endpoint due to error: {}".format(e))
                return
            self.ipython_display.writeln("Cleaned up endpoint {}".format(url))
            self.refresh_method()

        cleanup_w = self.ipywidget_factory.get_button(description="Clean Up")
        cleanup_w.on_click(cleanup_on_click)

        return cleanup_w

    def get_delete_button_endpoint(self, url, endpoint):
        def delete_on_click(button):
            self.endpoints.pop(url, None)
            self.refresh_method()

        delete_w = self.ipywidget_factory.get_button(description="Remove")
        delete_w.on_click(delete_on_click)

        return delete_w

    def get_delete_session_endpoint_widget(self, url, endpoint):
        session_text = self.ipywidget_factory.get_text(description="Session to delete:", value="0", width="50px")

        def delete_endpoint(button):
            try:
                id = session_text.value
                self.spark_controller.delete_session_by_id(endpoint, id)
                self.ipython_display.writeln("Deleted session {} at {}".format(id, url))
            except ValueError as e:
                self.ipython_display.send_error(str(e))
                return
            self.refresh_method()

        button = self.ipywidget_factory.get_button(description="Delete")
        button.on_click(delete_endpoint)

        return self.ipywidget_factory.get_hbox(children=[session_text, button], width="152px")

    def get_info_endpoint_widget(self, endpoint, url):
        # 400 px
        width = "400px"

        info_sessions = self.spark_controller.get_all_sessions_endpoint_info(endpoint)

        if len(info_sessions) > 0:
            text = "{}:<br/>{}".format(url, "* {}".format("<br/>* ".join(info_sessions)))
        else:
            text = "No sessions on this endpoint."

        return self.ipywidget_factory.get_html(text, width=width)
