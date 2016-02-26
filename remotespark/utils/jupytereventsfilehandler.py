import logging

from remotespark.utils.utils import join_paths, get_magics_home_path, get_instance_id
from remotespark.utils.filesystemreaderwriter import FileSystemReaderWriter


class EventsFileHandler(logging.FileHandler):
    """The default logging handler used by the magics; this behavior can be overridden by modifying the config file"""
    def __init__(self):
        # Simply invokes the behavior of the superclass, but sets the filename keyword argument if it's not already set.

        magics_home_path = get_magics_home_path()
        events_folder_name = "events"
        events_file_name = "events{}.log".format(get_instance_id())
        directory = FileSystemReaderWriter(join_paths(magics_home_path, events_folder_name))
        directory.ensure_path_exists()
        events_file = join_paths(directory.path, events_file_name)

    def emit(self, **kwargs):
        #Write the events to the local file here


