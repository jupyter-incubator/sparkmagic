from datetime import datetime
import importlib
from hdijupyterutils.constants import EVENT_NAME, TIMESTAMP
from hdijupyterutils.events import Events

from .constants import GRAPH_TYPE, GRAPH_RENDER_EVENT
from . import configuration as conf


class AutoVizEvents(Events):
    def __init__(self):
        handler = conf.events_handler()
        self.emit = handler is not None
        super(AutoVizEvents, self).__init__(handler)

    def emit_graph_render_event(self, graph_type):
        event_name = GRAPH_RENDER_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (GRAPH_TYPE, graph_type)]
             
        if self.emit:                  
            self.send_to_handler(kwargs_list)
