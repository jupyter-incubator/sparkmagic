from datetime import datetime
import importlib
from hdijupyterutils.constants import EVENT_NAME, TIMESTAMP
from hdijupyterutils.events import Events

from .constants import GRAPH_TYPE, GRAPH_RENDER_EVENT, AUTOVIZ_LOGGER_NAME
from . import configuration  as conf


class AutoVizEvents(Events):
    def __init__(self):
        """
        Create an instance from the handler mentioned in the config file.
        """
        module, class_name = conf.events_handler_class().rsplit('.', 1)
        events_handler_module = importlib.import_module(module)
        events_handler = getattr(events_handler_module, class_name)
        handler = events_handler(AUTOVIZ_LOGGER_NAME, conf.logging_config())
        
        super(AutoVizEvents, self).__init__(handler)

    def emit_graph_render_event(self, graph_type):
        event_name = GRAPH_RENDER_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (GRAPH_TYPE, graph_type)]
                       
        self.send_to_handler(kwargs_list)
