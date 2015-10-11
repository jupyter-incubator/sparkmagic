from __future__ import print_function
import ipywidgets as widgets
from ipywidgets import Button, Checkbox, Dropdown, Text, ToggleButtons, Textarea
from IPython.display import display

class AbstractMenuWidget(object):
    def __init__(self, spark_magic):
        self.spark_magic = spark_magic

    def _repr_html_(self):
        self._init_widgets()
        for child in self.children:
            display(child)
        return ""

    def hide_all(self):
        for child in self.children:
            child.visible = False

    def run(self):
        raise NotImplementedError("Concrete menu widget must define run")

    def __repr__(self):
        raise NotImplementedError("Concrete menu widget must define __repr__ for console view")

    def _init_widgets(self):
        raise NotImplementedError("Concrete menu widget must define _init_widgets")

class SubmitButton(Button):
    def __init__(self, *args, **kwargs):
        super(SubmitButton, self).__init__(*args, **kwargs)
        self.on_click(self.sumbit_clicked)

    def sumbit_clicked(self, button):
        self.parent_widget.run()
        self.parent_widget.hide_all()

class AddEndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_magic):
        super(AddEndpointWidget, self).__init__(spark_magic)

    def _init_widgets(self):
        self.lang_widget = ToggleButtons(
            description='Language:',
            options=['scala', 'python'],
        ) 
        self.address_widget = Text(
            description='Address:',
            value='http://livyserver.com',
        )
        self.alias_widget = Text(
            description='Alias:',
            value='endpoint-name'
        )
        self.user_widget = Text(
            description='Username:',
            value='username'
        )
        self.password_widget = Text(
            description='Password:',
            value='password'
        )
        self.submit_widget = SubmitButton(
            description='Create Endpoint'
        )

        self.children = [self.lang_widget, self.address_widget, self.alias_widget, self.user_widget, self.password_widget, self.submit_widget]

        for child in self.children:
            child.parent_widget = self     

    def run(self):
        language = self.lang_widget.value
        alias = self.alias_widget.value
        connection_string = "url={};username={};password={}".format(self.address_widget.value, self.user_widget.value, self.password_widget.value)

        self.spark_magic.add_endpoint(alias, language, connection_string)

    def __repr__(self):
        return "console view (will prompt for password)"
        
class DeleteEndpointWidget(AbstractMenuWidget):
    def __init__(self, spark_magic):
        super(DeleteEndpointWidget, self).__init__(spark_magic)

    def _init_widgets(self):
        self.endpoint_widget = Dropdown(
            options=self.spark_magic.get_endpoints_list(),
            description="Endpoint: "
        )
        self.submit_widget = SubmitButton(
            description='Delete Endpoint'
        )

        self.children = [self.endpoint_widget, self.submit_widget]

        for child in self.children:
            child.parent_widget = self 

    def run(self):
        to_delete = self.endpoint_widget.value
        self.spark_magic.delete_endpoint(to_delete)

    def __repr__(self):
        return "console view"

class RunCellWidget(AbstractMenuWidget):

    def __init__(self, spark_magic):
        super(RunCellWidget, self).__init__(spark_magic)

    def _init_widgets(self):
        self.endpoint_widget = Dropdown(
            options=self.spark_magic.get_endpoints_list(),
            description="Endpoint:"
        )
        self.sql_widget=Checkbox(
            description="Use Spark SQL?",
            value=False
        )
        self.cell_widget = Textarea(
            description="Code:",
            value=""
        )
        self.submit_widget = SubmitButton(
            description="Run Code Remotely"
        )

        self.children = [self.endpoint_widget, self.sql_widget, self.cell_widget, self.submit_widget]

        for child in self.children:
            child.parent_widget = self 

    def run(self):
        endpoint = self.endpoint_widget.value
        sql = self.sql_widget.value
        cell = self.cell_widget.value

        print(self.spark_magic.run_cell(endpoint, sql, cell))

    def __repr__(self):
        return "console view"