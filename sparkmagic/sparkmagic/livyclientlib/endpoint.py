from .exceptions import BadUserDataException, BadUserConfigurationException
from sparkmagic.utils.constants import AUTHS_SUPPORTED
from sparkmagic.utils.constants import AUTH_KERBEROS_MUTUAL_SUPPORTED


class Endpoint(object):
    def __init__(self, url, auth,
                    username="",
                    password="",
                    krb_mutual_auth=None,
                    krb_host_override=None,
                    implicitly_added=False):
        if not url:
            raise BadUserDataException(u"URL must not be empty")
        if auth not in AUTHS_SUPPORTED:
            raise BadUserConfigurationException(u"Auth '{}' not supported".format(auth))
        if krb_mutual_auth is not None and krb_mutual_auth not in AUTH_KERBEROS_MUTUAL_SUPPORTED:
            raise BadUserConfigurationException(u"Kerberos mutual auth '{}' not supported".format(krb_mutual_auth))
        self.url = url.rstrip(u"/")
        self.username = username
        self.password = password
        self.auth = auth
        # kerberos auth details for requests_kerberos
        self.krb_mutual_auth = krb_mutual_auth
        self.krb_host_override = krb_host_override
        # implicitly_added is set to True only if the endpoint wasn't configured manually by the user through
        # a widget, but was instead implicitly defined as an endpoint to a wrapper kernel in the configuration
        # JSON file.
        self.implicitly_added = implicitly_added

    def __eq__(self, other):
        if type(other) is not Endpoint:
            return False
        return (self.url == other.url and self.username == other.username 
                and self.password == other.password and self.auth == other.auth
                and self.krb_mutual_auth == other.krb_mutual_auth
                and self.krb_host_override == other.krb_host_override)

    def __hash__(self):
        return hash((self.url, self.username, self.password,
                    self.auth, self.krb_mutual_auth, self.krb_host_override))

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return u"Endpoint({})".format(self.url)
