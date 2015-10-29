# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from threading import Timer

from .log import Log
from .configuration import get_configuration
from .constants import Constants


class ClientManager(object):
    """Livy client manager"""

    def __init__(self, serializer=None):
        serialize_periodically = False
        serialize_period = 3

        if serializer is not None:
            serialize_periodically = get_configuration(Constants.serialize_periodically, True)
            serialize_period = get_configuration(Constants.serialize_period_seconds, 3)

        self.logger = Log("ClientManager")

        self._livy_clients = dict()
        self._serializer = serializer
        self._serialize_timer = None

        if self._serializer is not None:
            for (name, client) in self._serializer.deserialize_state():
                self.add_client(name, client)

            if serialize_periodically:
                self._serialize_state_periodically(serialize_period)

    def _serialize_state_periodically(self, serialize_period):
        self.logger.debug("Starting state serialize timer.")

        self._serialize_timer = Timer(serialize_period, self._serialize_state)
        self._serialize_timer.start()

    def _serialize_state(self):
        self._serializer.serialize_state(self._livy_clients)

    def get_endpoints_list(self):
        return list(self._livy_clients.keys())

    def add_client(self, name, livy_client):
        if name in self.get_endpoints_list():
            raise ValueError("Endpoint with name '{}' already exists. Please delete the endpoint"
                             " first if you intend to replace it.".format(name))

        self._livy_clients[name] = livy_client

    def get_any_client(self):
        number_of_sessions = len(self._livy_clients)
        if number_of_sessions == 1:
            key = self.get_endpoints_list()[0]
            return self._livy_clients[key]
        elif number_of_sessions == 0:
            raise AssertionError("You need to have at least 1 client created to execute commands.")
        else:
            raise AssertionError("Please specify the client to use. Possible endpoints are {}".format(
                self.get_endpoints_list()))
        
    def get_client(self, name):
        if name in self.get_endpoints_list():
            return self._livy_clients[name]
        raise ValueError("Could not find '{}' endpoint in list of saved endpoints. Possible endpoints are {}".format(
            name, self.get_endpoints_list()))

    def delete_client(self, name):
        self._remove_endpoint(name)
    
    def clean_up_all(self):
        for name in self.get_endpoints_list():
            self._remove_endpoint(name)

        if self._serializer is not None:
            self._serialize_state()

    def _remove_endpoint(self, name):
        if name in self.get_endpoints_list():
            self._livy_clients[name].close_session()
            del self._livy_clients[name]
        else:
            raise ValueError("Could not find '{}' endpoint in list of saved endpoints. Possible endpoints are {}"
                             .format(name, self.get_endpoints_list()))
