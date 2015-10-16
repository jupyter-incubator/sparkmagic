from nose.tools import raises
from mock import MagicMock

from remotespark.livyclientlib.clientmanagerstateserializer import ClientManagerStateSerializer


def test_deserialize_not_emtpy():
    client_factory = MagicMock()
    path = "resources/managerstate.json"
    serializer = ClientManagerStateSerializer(path, client_factory)

    assert serializer.path_to_serialized_state == path

    list = serializer.deserialize_state()

    assert len(list) == 2

    (name, client) = list[0]
    assert name == "py"
    (name, client) = list[1]
    assert name == "sc"


def test_deserialize_empty():
    client_factory = MagicMock()
    path = "resources/empty.json"
    serializer = ClientManagerStateSerializer(path, client_factory)

    assert serializer.path_to_serialized_state == path

    list = serializer.deserialize_state()

    assert len(list) == 0
