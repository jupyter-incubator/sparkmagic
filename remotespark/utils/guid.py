from remotespark.utils import utils


class GuidMixin(object):
    def __init__(self):
        self.guid = utils.generate_uuid()
