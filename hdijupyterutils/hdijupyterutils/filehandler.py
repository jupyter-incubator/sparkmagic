import logging

from .utils import join_paths, get_instance_id
from .filesystemreaderwriter import FileSystemReaderWriter


class MagicsFileHandler(logging.FileHandler):
    """The default logging handler used by the magics; this behavior can be overridden by modifying the config file"""
    def __init__(self, **kwargs):
        # Simply invokes the behavior of the superclass, but sets the filename keyword argument if it's not already set.
        if 'filename' in kwargs:
            super(MagicsFileHandler, self).__init__(**kwargs)
        else:
            magics_home_path = kwargs.pop(u"home_path")
            logs_folder_name = "logs"
            log_file_name = "log_{}.log".format(get_instance_id())
            directory = FileSystemReaderWriter(join_paths(magics_home_path, logs_folder_name))
            directory.ensure_path_exists()
            super(MagicsFileHandler, self).__init__(filename=join_paths(directory.path, log_file_name), **kwargs)

