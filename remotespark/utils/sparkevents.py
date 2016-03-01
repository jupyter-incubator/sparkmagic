import remotespark.utils.configuration as conf


class SparkEvents:
    def __init__(self):
        """
        Create an instance from the handler mentioned in the config file.
        """
        events_handler = self._import_handler(conf.events_config()["handler"])
        self.handler = events_handler()

    def _import_handler(self, name):
        """
        Importing the handler based on the passed parameter.
        """
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

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
