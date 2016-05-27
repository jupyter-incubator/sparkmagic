# Distributed under the terms of the Modified BSD License.
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from .constants import HOME_PATH, CONFIG_FILE
    

d = {}
path = join_paths(HOME_PATH, CONFIG_FILE)


def override(config, value):
    global d, path
    _override(d, path, config, value)


def override_all(obj):
    global d
    _override_all(d, obj)
    
    
# Configs

@with_override(d, path)
def events_handler_class():
    return EVENTS_HANDLER_CLASS_NAME

@with_override(d, path)
def max_slices_pie_graph():
    return 100
    
@with_override(d, path)
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
