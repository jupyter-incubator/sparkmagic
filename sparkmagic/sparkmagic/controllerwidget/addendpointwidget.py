# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from sparkmagic.livyclientlib.endpoint import Endpoint
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants


class AddEndpointWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(AddEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        widget_width = '300px'

        self.endpoints = endpoints
        self.endpoints_dropdown_widget = endpoints_dropdown_widget
        self.refresh_method = refresh_method

        self.url_mapping = {}
        if conf.endpoints():
            self.url_mapping = conf.endpoints()
            self.url_mapping['Other'] = 'OTHER'
        else:
            self.url_mapping['Other'] = 'OTHER'

        self.address_widget = self.ipywidget_factory.get_dropdown(
            options=self.url_mapping,
            description='Endpoint'
        )

        self.url_widget = self.ipywidget_factory.get_text(
            description='URL',
            value='https://address:8998',
            min_width='500px'
        )

        self.user_widget = self.ipywidget_factory.get_text(
            description='Username',
            value='username',
            width=widget_width
        )

        self.password_widget = self.ipywidget_factory.get_password(
            description='Password+VIP',
            style=dict(description_width='initial'),
            width=widget_width
        )

        # Submit widget
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Add endpoint'
        )

        self.auth_widget = self.ipywidget_factory.get_dropdown(
            options={constants.AUTH_KERBEROS: constants.AUTH_KERBEROS,
                     constants.AUTH_BASIC: constants.AUTH_BASIC,
                     constants.NO_AUTH: constants.NO_AUTH},
            description='Auth type'
        )

        if len(self.url_mapping) > 1:
            self.address_widget.layout.display = 'flex'
        else:
            self.address_widget.layout.display = 'none'

        if not conf.authentication_type():
            self.auth_widget.layout.display = 'flex'
        else:
            self.auth_widget.value = conf.authentication_type()
            self.auth_widget.layout.display = 'none'

        self.address_widget.on_trait_change(self._show_correct_endpoint_fields)
        self.auth_widget.on_trait_change(self._show_correct_endpoint_fields)

        self.children = [self.address_widget, self.url_widget, self.auth_widget, self.user_widget, self.password_widget,
                         self.ipywidget_factory.get_html(value="<br/>", width=widget_width), self.submit_widget]

        for child in self.children:
            child.parent_widget = self

        self._show_correct_endpoint_fields()

    def run(self):
        endpoint_key = self.url_mapping.keys()[list(self.url_mapping.values()).index(self.address_widget.value)]
        if endpoint_key == 'Other':
            endpoint = Endpoint(self.url_widget.value, self.auth_widget.value, self.user_widget.value, self.password_widget.value)
            endpoint_url = self.url_widget.value
            self.endpoints[endpoint_url] = endpoint
        else:
            endpoint = Endpoint(self.address_widget.value, self.auth_widget.value, self.user_widget.value, self.password_widget.value)
            endpoint_url = self.address_widget.value
            self.endpoints[endpoint_key] = endpoint

        self.ipython_display.writeln("Added endpoint {}: {}".format(endpoint_key, endpoint_url))

        # We need to call the refresh method because drop down in Tab 2 for endpoints wouldn't refresh with the new
        # value otherwise.
        self.refresh_method()

    def _show_correct_endpoint_fields(self):
        if self.auth_widget.value == constants.NO_AUTH or self.auth_widget.value == constants.AUTH_KERBEROS:
            self.user_widget.layout.display = 'none'
            self.password_widget.layout.display = 'none'
        else:
            self.user_widget.layout.display = 'flex'
            self.password_widget.layout.display = 'flex'

        if self.address_widget.value == 'OTHER':
            self.url_widget.layout.display = 'flex'
        else:
            self.url_widget.layout.display = 'none'
