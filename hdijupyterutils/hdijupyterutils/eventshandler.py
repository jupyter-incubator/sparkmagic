from .log import Log


class EventsHandler(object):
    def __init__(self, logger_name, logging_config):
        self.logger = Log(logger_name, logging_config, "EventsHandler")

    def handle_event(self, kwargs_list):
        """
        Storing the Event details using the logger.
        """
        event_line = ",".join("{}: {}".format(key, arg) for key, arg in kwargs_list)
        self.logger.info(event_line)
