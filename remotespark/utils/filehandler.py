import logging

from remotespark.utils.utils import join_paths, get_magics_home_path, get_instance_id, ensure_path_exists


class MagicsFileHandler(logging.FileHandler):
    """The default logging handler used by the magics; this behavior can be overriden by modifying the config file"""
    def __init__(self, **kwargs):
        # Simply invokes the behavior of the superclass, but sets the filename keyword argument if it's not already set.
        if 'filename' in kwargs:
            super(MagicsFileHandler, self).__init__(**kwargs)
        else:
            magics_home_path = get_magics_home_path()
            logs_folder_name = "logs"
            log_file_name = "log_{}.log".format(get_instance_id())
            directory = join_paths(magics_home_path, logs_folder_name)
            ensure_path_exists(directory)
            super(MagicsFileHandler, self).__init__(filename=join_paths(directory, log_file_name), **kwargs)

