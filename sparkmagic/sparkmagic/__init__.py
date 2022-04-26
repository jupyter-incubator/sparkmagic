__version__ = "0.19.2"

from sparkmagic.serverextension.handlers import load_jupyter_server_extension


def _jupyter_server_extension_paths():
    return [{"module": "sparkmagic"}]


def _jupyter_nbextension_paths():
    return []
