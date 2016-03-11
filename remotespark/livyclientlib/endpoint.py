class Endpoint(object):
    def __init__(self, url, username="", password=""):
        if not url:
            raise ValueError("URL must not be empty")
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.authenticate = False if username == '' and password == '' else True

    def __eq__(self, other):
        if type(other) is not Endpoint:
            return False
        return self.url == other.url and self.username == other.username and self.password == other.password

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Endpoint({})".format(self.url)
