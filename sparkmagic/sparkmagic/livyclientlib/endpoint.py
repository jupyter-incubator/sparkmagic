from .exceptions import BadUserDataException, BadUserConfigurationException
from sparkmagic.utils.constants import AUTHS_SUPPORTED


class Endpoint(object):
    def __init__(self, url, auth, username="", password="", implicitly_added=False, ssl_info=None):
        if not url:
            raise BadUserDataException(u"URL must not be empty")
        if auth not in AUTHS_SUPPORTED:
            raise BadUserConfigurationException(u"Auth '{}' not supported".format(auth))
        
        self.url = url.rstrip(u"/")
        self.username = username
        self.password = password
        self.auth = auth
        self.ssl_info = ssl_info
        # implicitly_added is set to True only if the endpoint wasn't configured manually by the user through
        # a widget, but was instead implicitly defined as an endpoint to a wrapper kernel in the configuration
        # JSON file.
        self.implicitly_added = implicitly_added

    def __eq__(self, other):
        if type(other) is not Endpoint:
            return False
        return self.url == other.url and self.username == other.username and self.password == other.password and self.auth == other.auth and self.ssl_info == other.ssl_info

    def __hash__(self):
        return hash((self.url, self.username, self.password, self.auth, self.ssl_info))

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return u"Endpoint({})".format(self.url)

class SSLInfo(object):
    def __init__(self, client_cert, client_key, ssl_verify):
        self.client_cert = client_cert
        self.client_key = client_key
        self.ssl_verify = ssl_verify
    
    @property
    def cert(self):
        return (self.client_cert, self.client_key, )

    def __eq__(self, other):
        if type(other) is not SSLInfo:
            return False
        return self.client_cert == other.client_cert and self.client_key == other.client_key and self.ssl_verify == other.ssl_verify

    def __hash__(self):
        return hash((self.client_cert, self.client_key, self.ssl_verify))

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return u"SSLInfo(client_cert={}, client_key={}, ssl_verify={})".format(self.client_cert, self.client_key, self.ssl_verify)
