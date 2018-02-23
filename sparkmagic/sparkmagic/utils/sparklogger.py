# Distributed under the terms of the Modified BSD License.
from hdijupyterutils.log import Log

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME


class SparkLog(Log):
    def __init__(self, class_name, conf=None):
        if not conf:
            conf = conf.logging_config()
        super(SparkLog, self).__init__(MAGICS_LOGGER_NAME, conf, class_name)
