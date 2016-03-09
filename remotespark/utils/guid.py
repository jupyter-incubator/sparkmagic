from remotespark.utils import utils


class ObjectWithGuid(object):
    def __init__(self):
        self.guid = utils.generate_uuid()
