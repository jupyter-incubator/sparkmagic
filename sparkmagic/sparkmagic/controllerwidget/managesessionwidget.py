# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget


class ManageSessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, refresh_method):
        # This is nested
        super(ManageSessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        self.refresh_method = refresh_method

        self.children = self.get_existing_session_widgets()

        for child in self.children:
            child.parent_widget = self

    def run(self):
        self.refresh_method()

    def get_existing_session_widgets(self):
        session_widgets = []
        session_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))

        client_dict = self.spark_controller.get_managed_clients()
        if len(client_dict) > 0:
            # Header
            header = self.get_session_widget("Name", "Id", "Kind", "State", False)
            session_widgets.append(header)
            session_widgets.append(self.ipywidget_factory.get_html(value="<hr/>", width="600px"))

            # Sessions
            for name, session in client_dict.items():
                session_widgets.append(self.get_session_widget(name, session.id, session.kind, session.status))

            session_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            session_widgets.append(self.ipywidget_factory.get_html(value="No sessions yet.", width="600px"))

        return session_widgets

    def get_session_widget(self, name, session_id, kind, state, button=True):
        hbox = self.ipywidget_factory.get_hbox()

        name_w = self.ipywidget_factory.get_html(value=name, width="200px", padding="4px")
        id_w = self.ipywidget_factory.get_html(value=str(session_id), width="100px", padding="4px")
        kind_w = self.ipywidget_factory.get_html(value=kind, width="100px", padding="4px")
        state_w = self.ipywidget_factory.get_html(value=state, width="100px", padding="4px")

        if button:
            def delete_on_click(button):
                self.spark_controller.delete_session_by_name(name)
                self.refresh_method()

            delete_w = self.ipywidget_factory.get_button(description="Delete")
            delete_w.on_click(delete_on_click)
        else:
            delete_w = self.ipywidget_factory.get_html(value="", width="100px", padding="4px")

        hbox.children = [name_w, id_w, kind_w, state_w, delete_w]

        return hbox
