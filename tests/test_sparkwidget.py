from nose.tools import raises, assert_equals
from mock import MagicMock

from remotespark.SparkWidget import AddEndpointWidget, DeleteEndpointWidget

def fake_widgets(parent, widget_dict):
	for widget_name in widget_dict:
		fake_widget = MagicMock()
		fake_widget.value = widget_dict[widget_name]
		setattr(parent, widget_name, fake_widget)

def test_add_endpoint():
	spark_magic = MagicMock()
	spark_magic.add_endpoint = MagicMock()
	widget = AddEndpointWidget(spark_magic)
	widget_vals = {"lang_widget" : "fakelanguage", "address_widget" : "http://location", "alias_widget" : "fakealias",
				   "user_widget" : "fakeuser", "password_widget" : "fakepass"}
	fake_widgets(widget, widget_vals)

	widget.run()

	spark_magic.add_endpoint.assert_called_once_with("fakealias", "fakelanguage", "url=http://location;username=fakeuser;password=fakepass")

def test_delete_endpoint():
	spark_magic = MagicMock()
	spark_magic.add_endpoint = MagicMock()
	widget = DeleteEndpointWidget(spark_magic)
	widget_vals = {"endpoint_widget" : "fakeendpoint"}
	fake_widgets(widget, widget_vals)

	widget.run()

	spark_magic.delete_endpoint.assert_called_once_with("fakeendpoint")