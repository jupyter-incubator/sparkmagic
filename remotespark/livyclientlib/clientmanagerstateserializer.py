# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

import json

from requests import ConnectionError

from remotespark.utils.ipythondisplay import IpythonDisplay
from remotespark.utils.log import Log
from .livysession import LivySession
from .livyclient import LivyClient


class ClientManagerStateSerializer(object):
    """Livy client manager state serializer"""

    def __init__(self, reader_writer):
        assert reader_writer is not None

        self.logger = Log("ClientManagerStateSerializer")

        self._reader_writer = reader_writer

    def deserialize_state(self):
        self.logger.debug("Deserializing state.")

        clients_to_return = []

        lines = self._reader_writer.read_lines()
        line = ''.join(lines).strip()

        if line != '':
            self.logger.debug("Read content. Converting to JSON.")
            json_str = json.loads(line)
            clients = json_str["clients"]

            for client in clients:
                # Ignore version for now
                name = client["name"]
                session_id = client["id"]
                sql_context_created = client["sqlcontext"]
                kind = client["kind"].lower()
                connection_string = client["connectionstring"]

                session = LivySession.from_connection_string(connection_string, {"kind": kind},
                                                             IpythonDisplay,
                                                             session_id, sql_context_created)

                # Do not start session automatically. Just create it but skip is not existent.
                try:
                    # Get status to know if it's alive or not.
                    status = session.status
                    if not session.is_final_status(status):
                        self.logger.debug("Adding session {}".format(session_id))
                        client_obj = LivyClient(session)
                        clients_to_return.append((name, client_obj))
                    else:
                        self.logger.error("Skipping serialized session '{}' because session was in status {}."
                                          .format(session.id, status))
                except (ValueError, ConnectionError) as e:
                    self.logger.error("Skipping serialized session '{}' because {}".format(session.id, str(e)))
        else:
            self.logger.debug("Empty manager state found.")

        return clients_to_return

    def serialize_state(self, name_client_dictionary):
        self.logger.debug("Serializing state.")

        serialized_clients = []
        for name in list(name_client_dictionary.keys()):
            client = name_client_dictionary[name]
            serialized_client = client.serialize()
            serialized_client["name"] = name
            serialized_clients.append(serialized_client)

        serialized_str = json.dumps({"clients": serialized_clients})
        self._reader_writer.overwrite_with_line(serialized_str)
