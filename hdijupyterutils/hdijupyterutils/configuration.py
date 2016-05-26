"""Utility to read configs from file."""
# Distributed under the terms of the Modified BSD License.
import json
import sys

from .utils import join_paths
from .filesystemreaderwriter import FileSystemReaderWriter
from .constants import LOGGING_CONFIG_CLASS_NAME

_overrides = None

class Configuration(object):
    
    def __init__(self, home_path, config_file_name, fsrw_class=None):
        self.home_path = home_path
        self.config_file_name = config_file_name
        self.fsrw_class = fsrw_class
        
    @property
    def overrides(self):
        global _overrides
        return _overrides

    def initialize(self):
        """Checks if the configuration is initialized. If so, initializes the
        global configuration object by reading from the configuration
        file, overwriting the current set of overrides if there is one"""
        global _overrides
        if _overrides is None:
            self.load()

    def load(self):
        """Initializes the global configuration by reading from the configuration
        file, overwriting the current set of overrides if there is one"""
        if self.fsrw_class is None:
            self.fsrw_class = FileSystemReaderWriter
        home_path = self.fsrw_class(self.home_path)
        home_path.ensure_path_exists()
        config_file = self.fsrw_class(join_paths(home_path.path, self.config_file_name))
        config_file.ensure_file_exists()
        config_text = config_file.read_lines()
        line = u"".join(config_text).strip()
        if line == u"":
            overrides = {}
        else:
            overrides = json.loads(line)
        self.override_all(overrides)

    def override_all(self, obj):
        """Given a dictionary representing the overrided defaults for this
        configuration, initialize the global configuration."""
        global _overrides
        _overrides = obj

    def override(self, config, value):
        """Given a string representing a configuration and a value for that configuration,
        override the configuration. Initialize the overrided configuration beforehand."""
        global _overrides
        self.initialize()
        _overrides[config] = value

    @staticmethod
    def _override(f):
        """A decorator which first initializes the overrided configurations,
        then checks the global overrided defaults for the given configuration,
        calling the function to get the default result otherwise."""
        global _overrides
        
        def ret(self):
            self.initialize()
            name = f.__name__
            if name in _overrides:
                return _overrides[name]
            else:
                return f()
        # Hack! We do this so that we can query the .__name__ of the function
        # later to get the name of the configuration dynamically, e.g. for unit tests
        ret.__name__ = f.__name__
        return ret
        
        
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
                u"home_path": "~/.hdijupyterutils"
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
