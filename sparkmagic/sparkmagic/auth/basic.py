"""Class for implementing a basic access authenticator for SparkMagic"""

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
from requests.auth import HTTPBasicAuth
from .customauth import Authenticator

class Basic(HTTPBasicAuth, Authenticator):
    """Basic Access authenticator for SparkMagic"""
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
            if parsed_attributes.user is '' or parsed_attributes.password is '':
                new_exc = BadUserDataException("Need to supply username and password arguments for "\
                    "Basic Access Authentication. (e.g. -a username -p password).")
                raise new_exc
            self.username = parsed_attributes.user
            self.password = parsed_attributes.password
        else:
            self.username = 'username'
            self.password = 'password'
        HTTPBasicAuth.__init__(self, self.username, self.password)
        Authenticator.__init__(self, parsed_attributes)

    def get_widgets(self, widget_width):
        """Creates and returns a list with an address, username, and password widget

        Args:
            widget_width (str): The width of all widgets to be created.

        Returns:
            Sequence[hdijupyterutils.ipywidgetfactory.IpyWidgetFactory]: list of widgets
        """
        ipywidget_factory = IpyWidgetFactory()

        self.user_widget = ipywidget_factory.get_text(
            description='Username:',
            value=self.username,
            width=widget_width
        )

        self.password_widget = ipywidget_factory.get_text(
            description='Password:',
            value=self.password,
            width=widget_width
        )

        widgets = [self.user_widget, self.password_widget]
        return Authenticator.get_widgets(self, widget_width) + widgets

    def update_with_widget_values(self):
        """Updates url, username, and password to be the value of their respective widgets."""
        Authenticator.update_with_widget_values(self)
        self.username = self.user_widget.value
        self.password = self.password_widget.value

    def __eq__(self, other):
        if not isinstance(other, Basic):
            return False
        return self.url == other.url and self.username == other.username and \
            self.password == other.password

    def __call__(self, request):
        return HTTPBasicAuth.__call__(self, request)

    def __hash__(self):
        return hash((self.username, self.password, self.url, self.__class__.__name__))
