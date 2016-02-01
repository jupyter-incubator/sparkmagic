# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json

from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.ipywidgetfactory import IpyWidgetFactory
from remotespark.utils.utils import get_connection_string
from remotespark.utils.constants import Constants
import remotespark.utils.configuration as conf


class AbstractMenuWidget(object):
    def __init__(self, spark_controller, ipywidget_factory=None, ipython_display=None):
        self.spark_controller = spark_controller

        if ipywidget_factory is None:
            ipywidget_factory = IpyWidgetFactory()
        self.ipywidget_factory = ipywidget_factory

        if ipython_display is None:
            ipython_display = IpythonDisplay()
        self.ipython_display = ipython_display

        self.children = []

    def _repr_html_(self):
        self._init_widgets()
        for child in self.children:
            self.ipython_display.display(child)
        return ""

    def hide_all(self):
        for child in self.children:
            child.visible = False

    def run(self):
        raise NotImplementedError("Concrete menu widget must define run")

    def _init_widgets(self):
        raise NotImplementedError("Concrete menu widget must define _init_widgets")


class MagicsControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display):
        super(MagicsControllerWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)

    def _init_widgets(self):
        self.add_session = AddSessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display)

        self.children = [self.add_session]

        for child in self.children:
            child.parent_widget = self

    def run(self):
        pass


class AddSessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display):
        super(AddSessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)

    def _init_widgets(self):
        self.lang_widget = self.ipywidget_factory.get_toggle_buttons(
            description='Language:',
            options=[Constants.lang_scala, Constants.lang_python],
        )
        self.address_widget = self.ipywidget_factory.get_text(
            description='Address:',
            value='http://example.com/livy',
        )
        self.session_widget = self.ipywidget_factory.get_text(
            description='Name:',
            value='session-name'
        )
        self.user_widget = self.ipywidget_factory.get_text(
            description='Username:',
            value='username'
        )
        self.password_widget = self.ipywidget_factory.get_text(
            description='Password:',
            value='password'
        )
        self.properties = self.ipywidget_factory.get_text(
            description='Properties:',
            value='{}'
        )
        self.submit_widget = self.ipywidget_factory.get_submit_button(
            description='Create Session'
        )

        self.error_message_widget = self.ipywidget_factory.get_text_area(
            description='Error:',
            value=''
        )
        self.error_message_widget.color = "red"
        self.error_message_widget.visible = False

        self.children = [self.lang_widget, self.address_widget, self.session_widget, self.user_widget,
                         self.password_widget, self.properties, self.submit_widget, self.error_message_widget]

        for child in self.children:
            child.parent_widget = self

    def run(self):
        self.error_message_widget.visible = False

        try:
            conf.override(conf.session_configs.__name__, json.loads(self.properties.value))
        except ValueError as e:
            self.error_message_widget.visible = True
            self.error_message_widget.value = "Session properties must be a valid JSON string. Error:\n{}".format(e)
            return

        language = self.lang_widget.value
        alias = self.session_widget.value
        connection_string = get_connection_string(self.address_widget.value, self.user_widget.value,
                                                  self.password_widget.value)
        skip = False

        properties = conf.get_session_properties(language)

        try:
            self.spark_controller.add_session(alias, connection_string, skip, properties)
        except ValueError as e:
            self.error_message_widget.visible = True
            self.error_message_widget.value = str(e)
            return
