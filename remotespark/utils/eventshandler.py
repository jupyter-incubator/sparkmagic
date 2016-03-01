from remotespark.utils.log import Log


class EventsHandler:
    def __init__(self):
        self.logger = Log("EventsHandler")

    def store_event(self, *args):
        """
        Storing the Event details using the logger.
        """
        event_line = ",".join("{}".format(arg) for arg in args)
        self.logger.debug(event_line)
