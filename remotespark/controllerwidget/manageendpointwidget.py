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
            header = self.get_endpoint_widget("Endpoint", None, False)
            endpoint_widgets.append(header)

            # Endpoints
            for url, conn_str in self.endpoints.items():
                endpoint_widgets.append(self.get_endpoint_widget(url, conn_str))

            endpoint_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            endpoint_widgets.append(self.ipywidget_factory.get_html(value="No endpoints yet.", width="600px"))

        return endpoint_widgets

    def get_endpoint_widget(self, url, conn_str, button=True):
        # 600 px
        vbox_outter = self.ipywidget_factory.get_vbox()
        hbox_outter = self.ipywidget_factory.get_hbox()

        separator = self.ipywidget_factory.get_html(value="<hr/>", width="600px")

        vbox_left = self.get_endpoint_left(conn_str, url)

        cleanup_w, delete_w = self.get_buttons_endpoint(url, conn_str, button)

        hbox_outter.children = [vbox_left, cleanup_w, delete_w]
        vbox_outter.children = [separator, hbox_outter]

        return vbox_outter

    def get_endpoint_left(self, conn_str, url):
        # 400 px
        if conn_str is None:
            return self.ipywidget_factory.get_text(value=url, width="200px")

        info = self.get_info_endpoint_widget(conn_str, url)
        delete_session_number = self.get_delete_session_endpoint_widget(url, conn_str)
        vbox_left = self.ipywidget_factory.get_vbox(children=[info, delete_session_number])
        return vbox_left

    def get_buttons_endpoint(self, url, conn_str, button):
        # 100 px x 2
        if button:
            def cleanup_on_click(button):
                try:
                    self.spark_controller.cleanup_endpoint(conn_str)
                except ValueError as e:
                    self.ipython_display.send_error("Could not clean up endpoint due to error: {}".format(e))
                    return
                self.ipython_display.writeln("Cleaned up endpoint {}".format(url))

            cleanup_w = self.ipywidget_factory.get_button(description="Clean Up")
            cleanup_w.on_click(cleanup_on_click)

            def delete_on_click(button):
                self.endpoints.pop(url, None)
                self.refresh_method()

            delete_w = self.ipywidget_factory.get_button(description="Remove")
            delete_w.on_click(delete_on_click)
        else:
            cleanup_w = self.ipywidget_factory.get_html(value="", width="100px", padding="4px")
            delete_w = self.ipywidget_factory.get_html(value="", width="100px", padding="4px")

        return cleanup_w, delete_w

    def get_delete_session_endpoint_widget(self, url, conn_str):
        # 400 px
        session_text = self.ipywidget_factory.get_text(description="Session:", value="0")

        def delete_endpoint(button):
            try:
                id = session_text.value
                self.spark_controller.delete_session_by_id(conn_str, id)
                self.ipython_display.writeln("Deleted session {} at {}".format(id, url))
            except ValueError as e:
                self.ipython_display.send_error(str(e))
                return

        button = self.ipywidget_factory.get_button(description="Delete")
        button.on_click(delete_endpoint)

        return self.ipywidget_factory.get_vbox(children=[session_text])

    def get_info_endpoint_widget(self, conn_str, url):
        # 400 px
        width = "400px"

        if conn_str is None:
            w = self.ipywidget_factory.get_text(value="", width=width)
        else:
            try:
                info = self.spark_controller.get_all_sessions_endpoint_info(conn_str)
                w = self.ipywidget_factory.get_html(value="{}: {}"
                                                    .format(url, info),
                                                    width=width)
            except ValueError as e:
                w = self.ipywidget_factory.get_text(value=str(e), width=width)

        return w
