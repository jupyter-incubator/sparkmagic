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
def events_handler():
    return None

@with_override(d, path)
def max_slices_pie_graph():
    return 100
