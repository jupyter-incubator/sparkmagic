# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json

from .log import Log


class ClientManagerStateSerializer(object):
    """Livy client manager state serializer"""
    logger = Log()

    def __init__(self, path_to_serialized_state, client_factory):
        if client_factory is None:
            raise ValueError("Client factory cannot be None")

        if path_to_serialized_state is None:
            raise ValueError("Path to serialize file cannot be None")

        self._path_to_serialized_state = path_to_serialized_state
        self._client_factory = client_factory

    @property
    def path_to_serialized_state(self):
        return self._path_to_serialized_state

    def deserialize_state(self, client_manager):
        if self.path_to_serialized_state is None:
            raise FileNotFoundError("Cannot deserialize state from None file.")

        self.logger.debug("Deserializing state from {}".format(self.path_to_serialized_state))

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
                    client_manager.add_client(name, client_obj)
            else:
                self.logger.debug("Empty manager state found at {}".format(self.path_to_serialized_state))

    def serialize_state(self):
        pass
