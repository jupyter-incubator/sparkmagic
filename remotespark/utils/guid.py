from remotespark.utils import utils


class GuidMixin:
    def __init__(self):
        self.guid = utils.generate_uuid()