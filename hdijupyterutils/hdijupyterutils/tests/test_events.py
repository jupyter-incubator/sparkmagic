import pytest
from mock import MagicMock

from hdijupyterutils.events import Events
from hdijupyterutils.utils import generate_uuid
from hdijupyterutils.constants import INSTANCE_ID, TIMESTAMP
from hdijupyterutils.utils import get_instance_id


def setup_function():
    global events, guid1, guid2, guid3, time_stamp

    events = Events(MagicMock())
    events.get_utc_date_time = MagicMock()
    time_stamp = events.get_utc_date_time()
    guid1 = generate_uuid()
    guid2 = generate_uuid()
    guid3 = generate_uuid()


def teardown_function():
    pass


def test_send_to_handler():
    kwargs_list = [(TIMESTAMP, time_stamp)]
    expected_kwargs_list = [(INSTANCE_ID, get_instance_id())] + kwargs_list

    events.send_to_handler(kwargs_list)

    events.handler.handle_event.assert_called_once_with(expected_kwargs_list)


def test_send_to_handler_asserts_less_than_12():
    with pytest.raises(AssertionError):
        kwargs_list = [(TIMESTAMP, time_stamp)] * 13
        events.send_to_handler(kwargs_list)
        assert False
