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
    _override(d, path, config, value)


def override_all(obj):
    _override_all(d, obj)
    
_with_override = with_override(d, path)
    
# Configs

@_with_override
def events_handler():
    return None

@_with_override
def max_slices_pie_graph():
    return 100
