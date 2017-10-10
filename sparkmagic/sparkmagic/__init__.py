__version__ = '0.12.5'

from sparkmagic.serverextension.handlers import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{
        "module": "sparkmagic"
    }]

def _jupyter_nbextension_paths():
    return []
