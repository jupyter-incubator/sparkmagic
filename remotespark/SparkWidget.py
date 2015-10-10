from __future__ import print_function
import ipywidgets as widgets
from IPython.display import display


class AddEndpointWidget(object):


    def __init__(self, spark_magic):
        self._notebook = False
        self.spark_magic = spark_magic

    def _init_widgets(self):
        self.lang = widgets.ToggleButtons(
            description='Language:',
            options=['scala', 'python'],
        )
        self.address = widgets.Text(
            description='Address:',
            value='http://livyserver.com',
        )
        self.alias = widgets.Text(
            description='Alias:',
            value='endpoint-name'
        )
        self.user = widgets.Text(
            description='Username:',
            value='username'
        )
        self.password = widgets.Text(
            description='Password:',
            value='password'
        )    

        self.submit = widgets.Button(description='Create Endpoint')
        def on_click(button):
            button.parent_widget.run()
            button.parent_widget.hide_all()
        self.submit.on_click(on_click)

        self.children = [self.lang, self.address, self.alias, self.user, self.password, self.submit]

        for child in self.children:
            child.parent_widget = self

        

    def run(self):
        language = self.lang.value
        name = self.alias.value
        connection_string = "url={};username={};password={}".format(self.address.value, self.user.value, self.password.value)

        self.spark_magic.add_endpoint(name, language, connection_string)

    def _repr_html_(self):
        self._init_widgets()
        self._notebook = True
        for child in self.children:
            display(child)
        return ""

    def __repr__(self):
        return "console view (will prompt for password)"
        

    def hide_all(self):
        for child in self.children:
            child.visible = False

