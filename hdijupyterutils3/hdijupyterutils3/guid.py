from .utils import generate_uuid


class ObjectWithGuid(object):
    def __init__(self):
        self.guid = generate_uuid()
