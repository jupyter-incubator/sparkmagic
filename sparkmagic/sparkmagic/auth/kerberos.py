"""Class for implementing a Kerberos authenticator for SparkMagic"""

from requests_kerberos import HTTPKerberosAuth
import sparkmagic.utils.configuration as conf
from .customauth import Authenticator


class Kerberos(HTTPKerberosAuth, Authenticator):
    """Kerberos authenticator for SparkMagic"""

    def __init__(self, parsed_attributes=None):
        """Initializes the Authenticator with the attributes in the attributes
        parsed from a %spark magic command if applicable, or with default values
        otherwise.

        Args:
            self,
            parsed_attributes (IPython.core.magics.namespace): The namespace object that
            is created from parsing %spark magic command.
        """
        HTTPKerberosAuth.__init__(self, **conf.kerberos_auth_configuration())
        Authenticator.__init__(self, parsed_attributes)

    def __call__(self, request):
        return HTTPKerberosAuth.__call__(self, request)

    def __hash__(self):
        return hash((self.url, self.__class__.__name__))
