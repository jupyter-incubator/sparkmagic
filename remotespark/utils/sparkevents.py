from datetime import datetime
import importlib
import remotespark.utils.configuration as conf
import remotespark.utils.constants as constants


class SparkEvents:
    def __init__(self):
        """
        Create an instance from the handler mentioned in the config file.
        """
        components = conf.events_handler_class().rsplit('.', 1)
        events_handler = importlib.import_module(components[0])
        my_e = getattr(events_handler, components[1])
        self.handler = my_e()


    def emit_start_session_event(self, session_guid, language):
        """
        Emitting Start Session Event
        """
        assert language in constants.SESSION_KINDS_SUPPORTED

        event_name = "notebookSessionStart"
        time_stamp = datetime.now()

        args = [("TimeStamp", time_stamp), ("EventName", event_name), ("SessionGuid", session_guid),
                ("SparkLanguage", language)]

        self.handler.handle_event(args)

    def emit_end_session_event(self, session_guid, language, session_id):
        """
        Emitting End Session Event
        """
        assert language in constants.SESSION_KINDS_SUPPORTED
        assert session_id >= 0

        args = []

        event_name = "notebookSessionStart"
        time_stamp = datetime.now()

        args = [("TimeStamp", time_stamp), ("EventName", event_name), ("SessionGuid", session_guid),
                ("SparkLanguage", language), ("SessionId", session_id)]

        self.handler.handle_event(args)
