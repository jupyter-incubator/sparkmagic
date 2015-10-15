# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json
from threading import Timer

from .log import Log


class ClientManager(object):
    """Livy client manager"""
    logger = Log()

    def __init__(self, client_factory, path_to_serialized_state=None,
                 serialize_periodically=False, serialize_period=30.0):
        if client_factory is None:
            raise ValueError("Client factory cannot be None")

        if path_to_serialized_state is None and serialize_periodically is True:
            raise ValueError("Will not be able to serialize periodically without path to serialize to.")

        self.livy_clients = dict()
        self._client_factory = client_factory
        self._path_to_serialized_state = path_to_serialized_state
        self._serialize_timer = None

        if self.path_to_serialized_state is not None:
            self.deserialize_state()

            if serialize_periodically:
                self.serialize_state_periodically(serialize_period)

    @property
    def path_to_serialized_state(self):
        return self._path_to_serialized_state

    def serialize_state_periodically(self, serialize_period):
        self._serialize_timer = Timer(serialize_period, self.serialize_state)
        self._serialize_timer.start()

    def serialize_state(self):
        pass

    def deserialize_state(self):
        self.logger.debug("Deserializing state from {}".format(self.path_to_serialized_state))

        if self.path_to_serialized_state is None:
            raise FileNotFoundError("Cannot deserialize state from None file.")

        with open(self.path_to_serialized_state, 'r') as f:
            lines = f.readlines()
            line = ''.join(lines).strip()

            if line != '':
                self.logger.debug("Read content. Converting to JSON.")
                json_str = json.loads(line)
                clients = json_str["clients"]

                for client in clients:
                    name = client["name"]
                    language = client["language"]
                    connection_string = client["connectionstring"]

                    # Do not start session automatically. Just populate it.
                    client_obj = self._client_factory.build_client(connection_string, language, False)
                    self.add_client(name, client_obj)
            else:
                self.logger.debug("Empty manager state found at {}".format(self.path_to_serialized_state))

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
