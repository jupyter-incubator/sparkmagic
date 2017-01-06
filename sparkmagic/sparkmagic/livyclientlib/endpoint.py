from .exceptions import BadUserDataException
from sparkmagic.utils.constants import NONE_AUTH

class Endpoint(object):
    def __init__(self, url, authentication=NONE_AUTH, username="", password=""):
        if not url:
            raise BadUserDataException(u"URL must not be empty")
        self.url = url.rstrip(u"/")
        self.authentication = authentication
        self.username = username
        self.password = password

    def __eq__(self, other):
        if type(other) is not Endpoint:
            return False
        return self.url == other.url and self.username == other.username and self.password == other.password

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return u"Endpoint({})".format(self.url)
