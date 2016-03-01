import remotespark.utils.configuration as conf
import importlib

class SparkEvents:
    def __init__(self):
        """
        Create an instance from the handler mentioned in the config file.
        """
        events_handler = importlib.import_module(conf.events_handler_class())
        self.handler = events_handler()

    def emit_start_session_event(self, time_stamp, event_name, session_guid, language):
        """
        Emitting Start Session Event
        """
        raise NotImplemented

    def emit_end_session_event(self,time_stamp, event_name, session_guid, language, session_id):
        """
        Emitting End Session Event
        """
        raise NotImplemented
