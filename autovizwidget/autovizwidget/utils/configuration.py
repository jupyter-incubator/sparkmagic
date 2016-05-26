"""Utility to read configs for autovizwidget.
"""
# Distributed under the terms of the Modified BSD License.
from hdijupyterutils.configuration import Configuration
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME

from .constants import HOME_PATH, CONFIG_FILE
    

class AutoVizConfiguration(Configuration):
    def __init__(self):
        super(AutoVizConfiguration, self).__init__(HOME_PATH, CONFIG_FILE)
        
    @Configuration._override
    def events_handler_class():
        return EVENTS_HANDLER_CLASS_NAME

    @Configuration._override
    def max_slices_pie_graph():
        return 100
        
    @Configuration._override
    def logging_config():
        return {
            u"version": 1,
            u"formatters": {
                u"magicsFormatter": {
                    u"format": u"%(asctime)s\t%(levelname)s\t%(message)s",
                    u"datefmt": u""
                }
            },
            u"handlers": {
                u"magicsHandler": {
                    u"class": LOGGING_CONFIG_CLASS_NAME,
                    u"formatter": u"magicsFormatter",
                    u"home_path": HOME_PATH
                }
            },
            u"loggers": {
                u"magicsLogger": {
                    u"handlers": [u"magicsHandler"],
                    u"level": u"DEBUG",
                    u"propagate": 0
                }
            }
        }
