from remotespark.utils.log import Log


class EventsHandler:
    def __init__(self):
        self.logger = Log("EventsHandler")

    def handle_event(self, args):
        """
        Storing the Event details using the logger.
        """
        event_line = ",".join("{}: {}".format(key, arg) for key, arg in args)
        self.logger.info(event_line)
