# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from ipywidgets import Layout, Label


class ManageSessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, refresh_method):
        # This is nested
        super(ManageSessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)

        box_layout = Layout(display='flex',
                            flex_flow='column',
                            align_items='stretch',
                            width='auto')

        self.refresh_method = refresh_method

        self.children = self.get_existing_session_widgets()
        self.layout = box_layout

        for child in self.children:
            child.parent_widget = self

    def run(self):
        self.refresh_method()

    def get_existing_session_widgets(self):
        session_widgets = []

        client_dict = self.spark_controller.get_managed_clients()
        if len(client_dict) > 0:
            # Header
            header = self.get_session_widget("Name", "ID", "Kind", "State", False)
            session_widgets.append(header)

            # Sessions
            for name, session in client_dict.items():
                session_widgets.append(self.get_session_widget(name, session.id, session.kind, session.status))

            session_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            session_widgets.append(self.ipywidget_factory.get_html(value="No sessions yet.", width="600px"))

        return session_widgets

    def get_session_widget(self, name, session_id, kind, state, button=True):

        hbox = self.ipywidget_factory.get_hbox(layout=Layout(display='flex',
                                                             flex_flow='row',
                                                             align_items='stretch',
                                                             width='auto'))

        button_layout = Layout(width="148px")
        name_w = self.ipywidget_factory.get_button(description=name, layout=button_layout, disabled=True)
        id_w = self.ipywidget_factory.get_button(description=str(session_id), layout=button_layout, disabled=True)
        kind_w = self.ipywidget_factory.get_button(description=kind, layout=button_layout, disabled=True)
        state_w = self.ipywidget_factory.get_button(description=state, layout=button_layout, disabled=True)

        if button:
            def delete_on_click(button):
                self.spark_controller.delete_session_by_name(name)
                self.refresh_method()

            delete_w = self.ipywidget_factory.get_button(description="Delete",
                                                         button_style='danger')
            delete_w.on_click(delete_on_click)
        else:
            delete_w = self.ipywidget_factory.get_button(description="Action",
                                                         button_style='primary', disabled=True)
            name_w.button_style = 'primary'
            id_w.button_style = 'primary'
            kind_w.button_style = 'primary'
            state_w.button_style = 'primary'

        hbox.children = [name_w, id_w, kind_w, state_w, delete_w]

        return hbox
