__version__ = '0.3.2'

from .handlers import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{
        "module": "sparkmagic"
    }]

def _jupyter_nbextension_paths():
    return []
