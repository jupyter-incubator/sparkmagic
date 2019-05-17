from datetime import datetime
import importlib

from hdijupyterutils3..constants import INSTANCE_ID
from hdijupyterutils3..utils import get_instance_id


class Events(object):
    def __init__(self, handler):
        self.handler = handler

    @staticmethod
    def get_utc_date_time():
        return datetime.utcnow()

    def send_to_handler(self, kwargs_list):
        kwargs_list = [(INSTANCE_ID, get_instance_id())] + kwargs_list

        assert len(kwargs_list) <= 12

        self.handler.handle_event(kwargs_list)
