"""Utility to read configs from file."""
# Distributed under the terms of the Modified BSD License.
import json
import sys

from .utils import join_paths
from .filesystemreaderwriter import FileSystemReaderWriter


def with_override(overrides, path, fsrw_class=None):
    """A decorator which first initializes the overrided configurations,
    then checks the global overrided defaults for the given configuration,
    calling the function to get the default result otherwise."""
    def ret(f):
        def wrapped_f(*args):
            # Can access overrides and path here
            _initialize(overrides, path, fsrw_class)
            name = f.__name__
            if name in overrides:
                return overrides[name]
            else:
                return f(*args)
        
        # Hack! We do this so that we can query the .__name__ of the function
        # later to get the name of the configuration dynamically, e.g. for unit tests
        wrapped_f.__name__ = f.__name__
        return wrapped_f
    
    return ret

    
def override(overrides, path, config, value, fsrw_class=None):
    """Given a string representing a configuration and a value for that configuration,
    override the configuration. Initialize the overrided configuration beforehand."""
    _initialize(overrides, path, fsrw_class)
    overrides[config] = value

    
def override_all(overrides, new_overrides):
    """Given a dictionary representing the overrided defaults for this
    configuration, initialize the global configuration."""
    overrides.clear()
    overrides.update(new_overrides)


def _initialize(overrides, path, fsrw_class):
    """Checks if the configuration is initialized. If it isn't, initializes the
    overrides object by reading from the configuration
    file, overwriting the current set of overrides if there is one."""
    if not overrides:
        new_overrides = _load(path, fsrw_class)
        override_all(overrides, new_overrides)
        
        
def _load(path, fsrw_class=None):
    """Returns a dictionary of configuration by reading from the configuration
    file."""
    if fsrw_class is None:
        fsrw_class = FileSystemReaderWriter
    
    config_file = fsrw_class(path)
    config_file.ensure_file_exists()
    config_text = config_file.read_lines()
    line = u"".join(config_text).strip()
    
    if line == u"":
        overrides = {}
    else:
        overrides = json.loads(line)
    return overrides
