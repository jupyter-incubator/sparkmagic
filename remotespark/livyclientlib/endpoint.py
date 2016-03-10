class Endpoint(object):
    def __init__(self, url, username, password):
        if not url:
            raise ValueError("URL must not be empty")
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.authenticate = False if username == '' and password == '' else True
