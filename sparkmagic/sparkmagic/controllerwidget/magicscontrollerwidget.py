# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from sparkmagic.controllerwidget.addendpointwidget import AddEndpointWidget
from sparkmagic.controllerwidget.manageendpointwidget import ManageEndpointWidget
from sparkmagic.controllerwidget.managesessionwidget import ManageSessionWidget
from sparkmagic.controllerwidget.createsessionwidget import CreateSessionWidget
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.utils.constants import LANGS_SUPPORTED
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.utils import Namespace, initialize_auth


class MagicsControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints=None):
        super(MagicsControllerWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)

        if endpoints is None:
            endpoints = {endpoint.url: endpoint for endpoint in self._get_default_endpoints()}
        self.endpoints = endpoints

        self._refresh()

    def run(self):
        pass

    @staticmethod
    def _get_default_endpoints():
        default_endpoints = set()

        for kernel_type in LANGS_SUPPORTED:
            endpoint_config = getattr(conf, 'kernel_%s_credentials' % kernel_type)()
            if all([p in endpoint_config for p in ["url", "password", "username"]]) and endpoint_config["url"] != "":
                user = endpoint_config["username"]
                passwd = endpoint_config["password"]
                args = Namespace(user=user, password=passwd, auth=endpoint_config.get("auth", None), url=endpoint_config.get("url", None))
                auth_instance = initialize_auth(args)

                default_endpoints.add(Endpoint(
                    auth=auth_instance,
                    url=endpoint_config["url"],
                    implicitly_added=True))

        return default_endpoints

    def _refresh(self):
        self.endpoints_dropdown_widget = self.ipywidget_factory.get_dropdown(
            description="Endpoint:",
            options=self.endpoints
        )

        self.manage_session = ManageSessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                                  self._refresh)
        self.create_session = CreateSessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                                  self.endpoints_dropdown_widget, self._refresh)
        self.add_endpoint = AddEndpointWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                              self.endpoints, self.endpoints_dropdown_widget, self._refresh)
        self.manage_endpoint = ManageEndpointWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                                    self.endpoints, self._refresh)

        self.tabs = self.ipywidget_factory.get_tab(children=[self.manage_session, self.create_session,
                                                             self.add_endpoint, self.manage_endpoint])
        self.tabs.set_title(0, "Manage Sessions")
        self.tabs.set_title(1, "Create Session")
        self.tabs.set_title(2, "Add Endpoint")
        self.tabs.set_title(3, "Manage Endpoints")

        self.children = [self.tabs]

        for child in self.children:
            child.parent_widget = self
