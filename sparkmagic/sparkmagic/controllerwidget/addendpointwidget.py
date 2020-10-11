# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import importlib
from sparkmagic.livyclientlib.endpoint import Endpoint
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import WIDGET_WIDTH
from .abstractmenuwidget import AbstractMenuWidget


class AddEndpointWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(AddEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.endpoints_dropdown_widget = endpoints_dropdown_widget
        self.refresh_method = refresh_method

        #map auth class path string to the instance of the class.
        self.auth_instances = {}
        for auth in conf.authenticators().values():
            module, class_name = (auth).rsplit('.', 1)
            events_handler_module = importlib.import_module(module)
            auth_class = getattr(events_handler_module, class_name)
            self.auth_instances[auth] = auth_class()

        self.auth_type = self.ipywidget_factory.get_dropdown(
            options=conf.authenticators(),
            description=u"Auth type:"
        )

        #combine all authentication instance's widgets into one list to pass to self.children.
        self.all_widgets = list()
        for _class, instance in self.auth_instances.items():
            for widget in instance.widgets:
                if  _class == self.auth_type.value:
                    widget.layout.display = 'flex'
                    self.auth = instance
                else:
                    widget.layout.display = 'none'
                self.all_widgets.append(widget)

        # Submit widget
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Add endpoint'
        )

        self.auth_type.on_trait_change(self._update_auth)

        self.children = [self.ipywidget_factory.get_html(value="<br/>", width=WIDGET_WIDTH), self.auth_type] + self.all_widgets \
        + [self.ipywidget_factory.get_html(value="<br/>", width=WIDGET_WIDTH), self.submit_widget]

        for child in self.children:
            child.parent_widget = self
        self._update_auth()

    def run(self):
        self.auth.update_with_widget_values()
        if self.auth_type.label == "None":
            endpoint = Endpoint(self.auth.url, None)
        else:
            endpoint = Endpoint(self.auth.url, self.auth)
        self.endpoints[self.auth.url] = endpoint
        self.ipython_display.writeln("Added endpoint {}".format(self.auth.url))
        try:
            # We need to call the refresh method because drop down in Tab 2 for endpoints wouldn't
            # refresh with the new value otherwise.
            self.refresh_method()
        except:
            self.endpoints.pop(self.auth.url, None)
            self.refresh_method()
            raise

    def _update_auth(self):
        """
        Create an instance of the chosen auth type maps to in the config file.
        """
        for widget in self.auth.widgets:
            widget.layout.display = 'none'
        self.auth = self.auth_instances.get(self.auth_type.value)
        for widget in self.auth.widgets:
            widget.layout.display = 'flex'
