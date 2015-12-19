from mock import MagicMock
from nose.tools import raises

from remotespark.livyclientlib.livyclientfactory import LivyClientFactory
from remotespark.livyclientlib.pandaspysparklivyclient import PandasPysparkLivyClient
from remotespark.livyclientlib.pandasscalalivyclient import PandasScalaLivyClient
from remotespark.utils.constants import Constants
from remotespark.utils.utils import get_connection_string


def test_build_session_with_defaults():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    kind = Constants.session_kind_pyspark
    properties = {"kind": kind}

    session = factory.create_session(connection_string, properties)

    assert session.kind == kind
    assert session.id == "-1"
    assert session.started_sql_context is False


def test_build_session():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    kind = Constants.session_kind_pyspark
    properties = {"kind": kind}

    session = factory.create_session(connection_string, properties, "1", True)

    assert session.kind == kind
    assert session.id == "1"
    assert session.started_sql_context


def test_can_build_all_clients():
    factory = LivyClientFactory()
    for kind in Constants.session_kinds_supported:
        session = MagicMock()
        session.kind = kind
        factory.build_client(session)


@raises(ValueError)
def test_build_unknown_language():
    session = MagicMock()
    session.kind = "unknown"
    factory = LivyClientFactory()
    factory.build_client(session)


def test_build_pyspark():
    session = MagicMock()
    session.kind = Constants.session_kind_pyspark
    factory = LivyClientFactory()
    client = factory.build_client(session)
    assert isinstance(client, PandasPysparkLivyClient)


def test_build_spark():
    session = MagicMock()
    session.kind = Constants.session_kind_spark
    factory = LivyClientFactory()
    client = factory.build_client(session)
    assert isinstance(client, PandasScalaLivyClient)
