import json

from mock import MagicMock
from nose.tools import with_setup, assert_equals, raises
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import (
    SessionManagementException,
    HttpClientException,
)
from sparkmagic.livyclientlib.sparkcontroller import SparkController

client_manager = None
controller = None
ipython_display = None


class DummyResponse:
    def __init__(self, status_code, json_text):
        self._status_code = status_code
        self._json_text = json_text

    def json(self):
        return json.loads(self._json_text)

    @property
    def status_code(self):
        return self._status_code


def _setup():
    global client_manager, controller, ipython_display

    client_manager = MagicMock()
    ipython_display = MagicMock()
    spark_events = MagicMock()
    controller = SparkController(ipython_display)
    controller.session_manager = client_manager
    controller.spark_events = spark_events


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_add_session():
    name = "name"
    properties = {"kind": "spark"}
    endpoint = Endpoint("http://location:port", None)
    session = MagicMock()

    controller._livy_session = MagicMock(return_value=session)
    controller._http_client = MagicMock(return_value=MagicMock())

    controller.add_session(name, endpoint, False, properties)

    controller._livy_session.assert_called_once_with(
        controller._http_client.return_value, properties, ipython_display
    )
    controller.session_manager.add_session.assert_called_once_with(name, session)
    session.start.assert_called_once()


@with_setup(_setup, _teardown)
def test_add_session_skip():
    name = "name"
    language = "python"
    connection_string = "url=http://location:port;username=name;password=word"
    client = "client"
    session = MagicMock()
    controller._livy_session = MagicMock(return_value=session)
    controller._http_client = MagicMock(return_value=client)

    client_manager.get_sessions_list.return_value = [name]
    controller.add_session(name, language, connection_string, True)

    assert controller._livy_session.create_session.call_count == 0
    assert controller._http_client.build_client.call_count == 0
    assert client_manager.add_session.call_count == 0
    assert session.start.call_count == 0


@with_setup(_setup, _teardown)
def test_delete_session():
    name = "name"

    controller.delete_session_by_name(name)

    client_manager.delete_client.assert_called_once_with(name)


@with_setup(_setup, _teardown)
def test_cleanup():
    controller.cleanup()
    client_manager.clean_up_all.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_run_cell():
    default_client = MagicMock()
    chosen_client = MagicMock()
    client_manager.get_any_session = MagicMock(return_value=default_client)
    client_manager.get_session = MagicMock(return_value=chosen_client)
    name = "session_name"
    command = MagicMock()

    controller.run_command(command, name)
    command.execute.assert_called_with(chosen_client)

    controller.run_command(command, None)
    command.execute.assert_called_with(default_client)


@with_setup(_setup, _teardown)
def test_run_sql():
    default_client = MagicMock()
    chosen_client = MagicMock()
    client_manager.get_any_session = MagicMock(return_value=default_client)
    client_manager.get_session = MagicMock(return_value=chosen_client)
    name = "session_name"
    sqlquery = MagicMock()

    controller.run_sqlquery(sqlquery, name)
    sqlquery.execute.assert_called_with(chosen_client)

    controller.run_sqlquery(sqlquery, None)
    sqlquery.execute.assert_called_with(default_client)


@with_setup(_setup, _teardown)
def test_get_client_keys():
    controller.get_client_keys()
    client_manager.get_sessions_list.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_get_all_sessions():
    http_client = MagicMock()
    http_client.get_sessions.return_value = json.loads(
        '{"from":0,"total":3,"sessions":[{"id":0,"state":"idle","kind":'
        '"spark","log":[""]}, {"id":1,"state":"busy","kind":"spark","log"'
        ':[""]},{"id":2,"state":"busy","kind":"sql","log"'
        ':[""]}]}'
    )
    controller._http_client = MagicMock(return_value=http_client)
    controller._livy_session = MagicMock()

    sessions = controller.get_all_sessions_endpoint("conn_str")

    assert len(sessions) == 2


@with_setup(_setup, _teardown)
def test_cleanup_endpoint():
    s0 = MagicMock()
    s1 = MagicMock()
    controller.get_all_sessions_endpoint = MagicMock(return_value=[s0, s1])

    controller.cleanup_endpoint("conn_str")

    s0.delete.assert_called_once_with()
    s1.delete.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_session_by_id_existent_non_managed():
    http_client = MagicMock()
    http_client.get_session.return_value = json.loads(
        '{"id":0,"state":"starting","kind":"spark","log":[]}'
    )
    controller._http_client = MagicMock(return_value=http_client)
    session = MagicMock()
    controller._livy_session = MagicMock(return_value=session)

    controller.delete_session_by_id("conn_str", 0)

    controller._livy_session.assert_called_once_with(
        http_client, {"kind": "spark"}, ipython_display, 0
    )
    session.delete.assert_called_once_with()


@with_setup(_setup, _teardown)
def test_delete_session_by_id_existent_managed():
    name = "name"
    controller.session_manager.get_session_name_by_id_endpoint = MagicMock(
        return_value=name
    )
    controller.session_manager.get_sessions_list = MagicMock(return_value=[name])
    controller.delete_session_by_name = MagicMock()

    controller.delete_session_by_id("conn_str", 0)

    controller.delete_session_by_name.assert_called_once_with(name)


@with_setup(_setup, _teardown)
@raises(HttpClientException)
def test_delete_session_by_id_non_existent():
    http_client = MagicMock()
    http_client.get_session.side_effect = HttpClientException
    controller._http_client = MagicMock(return_value=http_client)
    session = MagicMock()
    controller._livy_session = MagicMock(return_value=session)

    controller.delete_session_by_id("conn_str", 0)


@with_setup(_setup, _teardown)
def test_get_app_id():
    chosen_client = MagicMock()
    controller.get_session_by_name_or_default = MagicMock(return_value=chosen_client)

    result = controller.get_app_id()
    assert_equals(result, chosen_client.get_app_id.return_value)
    chosen_client.get_app_id.assert_called_with()


@with_setup(_setup, _teardown)
def test_get_driver_log():
    chosen_client = MagicMock()
    controller.get_session_by_name_or_default = MagicMock(return_value=chosen_client)

    result = controller.get_driver_log_url()
    assert_equals(result, chosen_client.get_driver_log_url.return_value)
    chosen_client.get_driver_log_url.assert_called_with()


@with_setup(_setup, _teardown)
def test_get_logs():
    chosen_client = MagicMock()
    controller.get_session_by_name_or_default = MagicMock(return_value=chosen_client)

    result = controller.get_logs()
    assert_equals(result, chosen_client.get_logs.return_value)
    chosen_client.get_logs.assert_called_with()


@with_setup(_setup, _teardown)
@raises(SessionManagementException)
def test_get_logs_error():
    chosen_client = MagicMock()
    controller.get_session_by_name_or_default = MagicMock(
        side_effect=SessionManagementException("THERE WAS A SPOOKY GHOST")
    )

    result = controller.get_logs()


@with_setup(_setup, _teardown)
def test_get_session_id_for_client():
    assert controller.get_session_id_for_client("name") is not None
    client_manager.get_session_id_for_client.assert_called_once_with("name")


@with_setup(_setup, _teardown)
def test_get_spark_ui_url():
    chosen_client = MagicMock()
    controller.get_session_by_name_or_default = MagicMock(return_value=chosen_client)

    result = controller.get_spark_ui_url()
    assert_equals(result, chosen_client.get_spark_ui_url.return_value)
    chosen_client.get_spark_ui_url.assert_called_with()


@with_setup(_setup, _teardown)
def test_add_session_throws_when_session_start_fails():
    name = "name"
    properties = {"kind": "spark"}
    endpoint = Endpoint("http://location:port", None)
    session = MagicMock()

    controller._livy_session = MagicMock(return_value=session)
    controller._http_client = MagicMock(return_value=MagicMock())
    e = ValueError("Failed to create the SqlContext.\nError, '{}'".format("Exception"))
    session.start = MagicMock(side_effect=e)

    try:
        controller.add_session(name, endpoint, False, properties)
        assert False
    except ValueError as ex:
        assert str(ex) == str(e)
        session.start.assert_called_once()
        controller.session_manager.add_session.assert_not_called


@with_setup(_setup, _teardown)
def test_add_session_cleanup_when_timeouts_and_session_posted_to_livy():
    pass


@with_setup(_setup, _teardown)
def test_add_session_cleanup_when_timeouts_and_session_posted_to_livy():
    _do_test_add_session_cleanup_when_timeouts(is_session_posted_to_livy=True)


@with_setup(_setup, _teardown)
def test_add_session_cleanup_when_timeouts_and_session_not_posted_to_livy():
    _do_test_add_session_cleanup_when_timeouts(is_session_posted_to_livy=False)


def _do_test_add_session_cleanup_when_timeouts(is_session_posted_to_livy):
    name = "name"
    properties = {"kind": "spark"}
    endpoint = Endpoint("http://location:port", None)
    session = MagicMock()

    controller._livy_session = MagicMock(return_value=session)
    controller._http_client = MagicMock(return_value=MagicMock())
    e = RuntimeError("Time out while post session to livy")
    session.start = MagicMock(side_effect=e)

    if is_session_posted_to_livy:
        session.is_posted = MagicMock(return_value=True)
    else:
        session.is_posted = MagicMock(return_value=False)

    try:
        controller.add_session(name, endpoint, False, properties)
        assert False
    except RuntimeError as ex:
        assert str(ex) == str(e)
        session.start.assert_called_once()
        controller.session_manager.add_session.assert_not_called

        if is_session_posted_to_livy:
            session.delete.assert_called_once()
        else:
            session.delete.assert_not_called()


@with_setup(_setup, _teardown)
def test_add_session_cleanup_when_session_delete_throws():
    name = "name"
    properties = {"kind": "spark"}
    endpoint = Endpoint("http://location:port", None)
    session = MagicMock()

    controller._livy_session = MagicMock(return_value=session)
    controller._http_client = MagicMock(return_value=MagicMock())
    e = RuntimeError("Time out while post session to livy")
    session.start = MagicMock(side_effect=e)
    session.is_posted = MagicMock(return_value=True)

    error_from_cleanup = RuntimeError("Error while clean up session")
    session.delete = MagicMock(side_effect=error_from_cleanup)
    try:
        controller.add_session(name, endpoint, False, properties)
        assert False
    except Exception as ex:
        # in the exception chain mechanism, original exception will be set as context.
        assert str(ex) == str(error_from_cleanup)
        assert str(ex.__context__) == str(e)
        controller.session_manager.add_session.assert_not_called
