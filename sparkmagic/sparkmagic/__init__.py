__version__ = '0.11.1'

from sparkmagic.serverextension.handlers import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{
        "module": "sparkmagic"
    }]

def _jupyter_nbextension_paths():
    return []
