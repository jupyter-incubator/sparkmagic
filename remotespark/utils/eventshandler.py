from remotespark.utils.log import Log


class EventsHandler:
    def __init__(self):
        self.logger = Log("EventsHandler")

    def store_event(self, **kwargs):
        """
        Storing the Event details using the logger.
        """
        event_line = ""
        for key, value in kwargs.iteritems():
            event_line += "{} = {}\n".format(key, value)
        self.logger.debug(event_line)
