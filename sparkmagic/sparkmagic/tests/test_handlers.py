from mock import MagicMock
from nose.tools import with_setup, raises, assert_equals, assert_is

from sparkmagic.handlers import ReconnectHandler
from sparkmagic.kernels.kernelmagics import KernelMagics


reconnect_handler = None
session_manager = None
kernel_manager = None
individual_kernel_manager = None
client = None
session_list = None
spark_events = None
path = 'some_path.ipynb'
kernel_id = '1'
username = 'username'
password = 'password'
endpoint = 'http://endpoint.com'
response_id = '0'
good_msg = dict(content=dict(status='ok'))
bad_msg = dict(content=dict(status='error', ename='SyntaxError', evalue='oh no!'))


def create_session_dict(path, kernel_id):
    return dict(notebook=dict(path=path), kernel=dict(id=kernel_id))


def get_argument(key):
    return dict(username=username, password=password, endpoint=endpoint, path=path)[key]


class SimpleObject(object):
    pass


def _setup():
    global reconnect_handler, session_manager, session_list, path, kernel_id, kernel_manager,\
     individual_kernel_manager, response_id, good_msg, client, spark_events

    # Mock kernel manager
    client = MagicMock()
    client.execute = MagicMock(return_value=response_id)
    client.get_shell_msg = MagicMock(return_value=good_msg)
    individual_kernel_manager = MagicMock()
    individual_kernel_manager.client = MagicMock(return_value=client)
    kernel_manager = MagicMock()
    kernel_manager.get_kernel = MagicMock(return_value=individual_kernel_manager)

    # Mock session manager
    session_list = [create_session_dict(path, kernel_id)]
    session_manager = MagicMock()
    session_manager.list_sessions = MagicMock(return_value=session_list)

    #Mock spark events
    spark_events = MagicMock()

    # Create mocked reconnect_handler        
    ReconnectHandler.__bases__ = (SimpleObject,)
    reconnect_handler = ReconnectHandler()
    reconnect_handler.spark_events = spark_events
    reconnect_handler.session_manager = session_manager
    reconnect_handler.kernel_manager = kernel_manager
    reconnect_handler.set_status = MagicMock()
    reconnect_handler.finish = MagicMock()
    reconnect_handler.get_body_argument = get_argument


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_msg_status():
    assert_equals(reconnect_handler.msg_status(good_msg), 'ok')
    assert_equals(reconnect_handler.msg_status(bad_msg), 'error')


@with_setup(_setup, _teardown)
def test_msg_successful():
    assert_equals(reconnect_handler.msg_successful(good_msg), True)
    assert_equals(reconnect_handler.msg_successful(bad_msg), False)


@with_setup(_setup, _teardown)
def test_msg_error():
    assert_equals(reconnect_handler.msg_error(good_msg), None)
    assert_equals(reconnect_handler.msg_error(bad_msg), u'{}:\n{}'.format('SyntaxError', 'oh no!'))


@with_setup(_setup, _teardown)
def test_get_kernel_manager():
    assert_equals(reconnect_handler.get_kernel_manager('not_existing_path.ipynb'), None)
    assert_equals(reconnect_handler.get_kernel_manager(path), individual_kernel_manager)

    kernel_manager.get_kernel.assert_called_once_with(kernel_id)


@with_setup(_setup, _teardown)
def test_post_non_existing_kernel():
    reconnect_handler.get_kernel_manager = MagicMock(return_value=None)

    assert reconnect_handler.post() is None

    reconnect_handler.set_status.assert_called_once_with(404)
    reconnect_handler.finish.assert_called_once_with('{"success": false, "error": "No kernel for given path"}')
    spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 404, False, "No kernel for given path")


@with_setup(_setup, _teardown)
def test_post_existing_kernel():
    assert reconnect_handler.post() is None

    individual_kernel_manager.restart_kernel.assert_called_once_with()
    code = '%{} -s {} -u {} -p {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password)
    client.execute.assert_called_once_with(code, silent=False, store_history=False)
    reconnect_handler.set_status.assert_called_once_with(200)
    reconnect_handler.finish.assert_called_once_with('{"success": true, "error": null}')
    spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 200, True, None)


@with_setup(_setup, _teardown)
def test_post_existing_kernel_failed():
    client.get_shell_msg = MagicMock(return_value=bad_msg)

    assert reconnect_handler.post() is None

    individual_kernel_manager.restart_kernel.assert_called_once_with()
    code = '%{} -s {} -u {} -p {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password)
    client.execute.assert_called_once_with(code, silent=False, store_history=False)
    reconnect_handler.set_status.assert_called_once_with(500)
    reconnect_handler.finish.assert_called_once_with('{"success": false, "error": "SyntaxError:\\noh no!"}')
    spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 500, False, "SyntaxError:\noh no!")
