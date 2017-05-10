from .exceptions import BadUserDataException
import sparkmagic.utils.configuration as conf


class Endpoint(object):
    def __init__(self, url, auth_type=conf.default_livy_endpoint_auth_type(), username="", password=""):
        if not url:
            raise BadUserDataException(u"URL must not be empty")
        self.url = url.rstrip(u"/")
        self.username = username
        self.password = password
        self.auth_type = auth_type

    def __eq__(self, other):
        if type(other) is not Endpoint:
            return False
        return self.url == other.url and self.username == other.username and self.password == other.password

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return u"Endpoint({})".format(self.url)
