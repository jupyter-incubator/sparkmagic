from remotespark.utils import utils


class GuidWithObject(object):
    def __init__(self):
        self.guid = utils.generate_uuid()
