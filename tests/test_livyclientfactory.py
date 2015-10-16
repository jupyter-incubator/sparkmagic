from mock import MagicMock
from nose.tools import raises

from remotespark.livyclientlib.livyclientfactory import LivyClientFactory
from remotespark.livyclientlib.connectionstringutil import get_connection_string
from remotespark.livyclientlib.constants import Constants


def test_build_session_with_defaults():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    language = "python"

    session = factory.create_session(language, connection_string)

    assert session.language == language
    assert session.id == "-1"
    assert session.started_sql_context is False


def test_build_session():
    factory = LivyClientFactory()
    connection_string = get_connection_string("url", "user", "pass")
    language = "python"

    session = factory.create_session(language, connection_string, "1", True)

    assert session.language == language
    assert session.id == "1"
    assert session.started_sql_context


def test_can_build_all_clients():
    session = MagicMock()
    factory = LivyClientFactory()
    for language in Constants.lang_supported:
        factory.build_client(language, session)


@raises(ValueError)
def test_build_unknown_language():
    session = MagicMock()
    factory = LivyClientFactory()
    factory.build_client("unknown", session)
