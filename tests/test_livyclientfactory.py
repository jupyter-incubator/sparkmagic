from mock import MagicMock
from nose.tools import raises

from remotespark.livyclientlib.livyclientfactory import LivyClientFactory
from remotespark.livyclientlib.pysparklivyclient import PysparkLivyClient
from remotespark.livyclientlib.scalalivyclient import ScalaLivyClient
from remotespark.utils.constants import SESSION_KIND_PYSPARK, SESSION_KINDS_SUPPORTED, SESSION_KIND_SPARK
from remotespark.utils.utils import get_connection_string


def test_build_session_with_defaults():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    kind = SESSION_KIND_PYSPARK
    properties = {"kind": kind}
    ipython_display = MagicMock()

    session = factory.create_session(ipython_display, connection_string, properties)

    assert session.kind == kind
    assert session.id == "-1"
    assert session.started_sql_context is False
    assert session.properties == properties


def test_build_session():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    kind = SESSION_KIND_PYSPARK
    properties = {"kind": kind}
    ipython_display = MagicMock()

    session = factory.create_session(ipython_display, connection_string, properties, "1", True)

    assert session.kind == kind
    assert session.id == "1"
    assert session.started_sql_context
    assert session.properties == properties


def test_can_build_all_clients():
    factory = LivyClientFactory()
    for kind in SESSION_KINDS_SUPPORTED:
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
    session.kind = SESSION_KIND_PYSPARK
    factory = LivyClientFactory()
    client = factory.build_client(session)
    assert isinstance(client, PysparkLivyClient)


def test_build_spark():
    session = MagicMock()
    session.kind = SESSION_KIND_SPARK
    factory = LivyClientFactory()
    client = factory.build_client(session)
    assert isinstance(client, ScalaLivyClient)
