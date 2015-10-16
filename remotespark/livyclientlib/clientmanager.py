# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from threading import Timer

from .log import Log


class ClientManager(object):
    """Livy client manager"""
    logger = Log()

    def __init__(self, serializer=None, serialize_periodically=False, serialize_period=10.0):
        if serializer is None and serialize_periodically is True:
            raise ValueError("Will not be able to serialize periodically without serializer.")

        self.livy_clients = dict()
        self._serializer = serializer
        self._serialize_timer = None

        if self._serializer is not None:
            self._serializer.deserialize_state(self)

            if serialize_periodically:
                self.serialize_state_periodically(serialize_period)

    def serialize_state_periodically(self, serialize_period):
        if self._serializer is None:
            raise ValueError("Cannot start periodic serialization if serializer is None.")

        self._serialize_timer = Timer(serialize_period, self._serializer.serialize_state)
        self._serialize_timer.start()

    def get_endpoints_list(self):
        return list(self.livy_clients.keys())

    def add_client(self, name, livy_client):
        if name in self.get_endpoints_list():
            raise ValueError("Endpoint with name '{}' already exists. Please delete the endpoint"
                             " first if you intend to replace it.".format(name))

        self.livy_clients[name] = livy_client

    def get_any_client(self):
        number_of_sessions = len(self.livy_clients)
        if number_of_sessions == 1:
            key = self.get_endpoints_list()[0]
            return self.livy_clients[key]
        elif number_of_sessions == 0:
            raise AssertionError("You need to have at least 1 client created to execute commands.")
        else:
            raise AssertionError("Please specify the client to use. Possible endpoints are {}".format(
                self.get_endpoints_list()))
        
    def get_client(self, name):
        if name in self.get_endpoints_list():
            return self.livy_clients[name]
        raise ValueError("Could not find '{}' endpoint in list of saved endpoints. Possible endpoints are {}".format(
            name, self.get_endpoints_list()))

    def delete_client(self, name):
        self._remove_endpoint(name)
    
    def clean_up_all(self):
        for name in self.get_endpoints_list():
            self._remove_endpoint(name)

    def _remove_endpoint(self, name):
        if name in self.get_endpoints_list():
            self.livy_clients[name].close_session()
            del self.livy_clients[name]
        else:
            raise ValueError("Could not find '{}' endpoint in list of saved endpoints. Possible endpoints are {}"
                             .format(name, self.get_endpoints_list()))
