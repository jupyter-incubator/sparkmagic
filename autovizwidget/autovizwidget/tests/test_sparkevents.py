from hdijupyterutils.constants import INSTANCE_ID, EVENT_NAME, TIMESTAMP
from hdijupyterutils.utils import get_instance_id
from nose.tools import with_setup, raises
from mock import MagicMock

from autovizwidget.utils.events import AutoVizEvents
from autovizwidget.utils.constants import GRAPH_RENDER_EVENT, GRAPH_TYPE
import autovizwidget.utils.configuration as conf


def _setup():
    global events, time_stamp

    events = AutoVizEvents()
    events.handler = MagicMock()
    events.get_utc_date_time = MagicMock()
    time_stamp = events.get_utc_date_time()


def _teardown():
    conf.override_all({})


@with_setup(_setup, _teardown)
def test_not_emit_graph_render_event_when_not_registered():
    event_name = GRAPH_RENDER_EVENT
    graph_type = "Bar"

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (GRAPH_TYPE, graph_type),
    ]

    events.emit_graph_render_event(graph_type)

    events.get_utc_date_time.assert_called_with()
    assert not events.handler.handle_event.called


@with_setup(_setup, _teardown)
def test_emit_graph_render_event_when_registered():
    conf.override(conf.events_handler.__name__, events.handler)
    event_name = GRAPH_RENDER_EVENT
    graph_type = "Bar"

    kwargs_list = [
        (INSTANCE_ID, get_instance_id()),
        (EVENT_NAME, event_name),
        (TIMESTAMP, time_stamp),
        (GRAPH_TYPE, graph_type),
    ]

    events.emit_graph_render_event(graph_type)

    events.get_utc_date_time.assert_called_with()
    assert not events.handler.handle_event.called
