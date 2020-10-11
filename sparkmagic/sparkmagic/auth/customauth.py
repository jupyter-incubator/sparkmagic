"""Base class for implementing an authentication provider for SparkMagic"""

from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
from sparkmagic.utils.constants import WIDGET_WIDTH

class Authenticator(object):
    """Base Authenticator for all Sparkmagic authentication providers."""

    def __init__(self, parsed_attributes=None):
        """Initializes the Authenticator with the attributes in the attributes
        parsed from a %spark magic command if applicable, or with default values
        otherwise.

        Args:
            self,
            parsed_attributes (IPython.core.magics.namespace): The namespace object that
            is created from parsing %spark magic command.
        """
        if parsed_attributes is not None:
            self.url = parsed_attributes.url
        else:
            self.url = 'http://example.com/livy'
        self.widgets = self.get_widgets(WIDGET_WIDTH)

    def get_widgets(self, widget_width):
        """Creates and returns an address widget

        Args:
            widget_width (str): The width of all widgets to be created.

        Returns:
            Sequence[hdijupyterutils.ipywidgetfactory.IpyWidgetFactory]: list of widgets
        """
        ipywidget_factory = IpyWidgetFactory()

        self.address_widget = ipywidget_factory.get_text(
            description='Address:',
            value='http://example.com/livy',
            width=widget_width
        )
        widgets = [self.address_widget]
        return widgets

    def update_with_widget_values(self):
        """Updates url to be value in address widget."""
        self.url = self.address_widget.value

    def __call__(self, request):
        """subclasses should override"""
        return None

    def __eq__(self, other):
        if not isinstance(other, Authenticator):
            return False
        return self.url == other.url

    def __hash__(self):
        return hash((self.url, self.__class__.__name__))
