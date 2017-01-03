from mock import MagicMock, patch
from nose.tools import with_setup, raises, assert_equals, assert_is
from tornado.concurrent import Future
from tornado.web import MissingArgumentError
from tornado.testing import gen_test
from tornado.testing import AsyncTestCase
import json

from sparkmagic.serverextension.handlers import ReconnectHandler
from sparkmagic.kernels.kernelmagics import KernelMagics
import sparkmagic.utils.configuration as conf


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
request = None


def create_session_dict(path, kernel_id):
    return dict(notebook=dict(path=path), kernel=dict(id=kernel_id))


def get_argument(key):
    return dict(username=username, password=password, endpoint=endpoint, path=path)[key]


class SimpleObject(object):
    pass


class Test123(AsyncTestCase):

    @patch('sparkmagic.serverextension.handlers.ReconnectHandler._get_kernel_manager_new_session')
    @gen_test
    def test_1(self, mock_func_inner_1):
        future_1 = Future()
        kernel_manager = MagicMock()
        future_1.set_result(kernel_manager)
        mock_func_inner_1.return_value = future_1
        #result_1 = yield _func_inner_1()
        ReconnectHandler.__bases__ = (SimpleObject,)
        handler = ReconnectHandler()
        result = yield handler._get_kernel_manager_new_session("a", "b")
        assert_equals(result, kernel_manager)

        
# def _setup():
#     global reconnect_handler, session_manager, session_list, path, kernel_id, kernel_manager,\
#      individual_kernel_manager, response_id, good_msg, client, spark_events, request

#     # Mock kernel manager
#     client = MagicMock()
#     client.execute = MagicMock(return_value=response_id)
#     client.get_shell_msg = MagicMock(return_value=good_msg)
#     individual_kernel_manager = MagicMock()
#     individual_kernel_manager.client = MagicMock(return_value=client)
#     kernel_manager = MagicMock()
#     kernel_manager.get_kernel = MagicMock(return_value=individual_kernel_manager)

#     # Mock session manager
#     session_list = [create_session_dict(path, kernel_id)]
#     session_manager = MagicMock()
#     session_manager.list_sessions = MagicMock(return_value=session_list)
#     session_manager.create_session = MagicMock(return_value=create_session_dict(path, kernel_id))

#     # Mock spark events
#     spark_events = MagicMock()

#     # Mock request 
#     request = MagicMock()
#     request.body = json.dumps({"path": path, "username": username, "password": password, "endpoint": endpoint})

#     # Create mocked reconnect_handler        
#     ReconnectHandler.__bases__ = (SimpleObject,)
#     reconnect_handler = ReconnectHandler()
#     reconnect_handler.spark_events = spark_events
#     reconnect_handler.session_manager = session_manager
#     reconnect_handler.kernel_manager = kernel_manager
#     reconnect_handler.set_status = MagicMock()
#     reconnect_handler.finish = MagicMock()
#     reconnect_handler.current_user = 'alex'
#     reconnect_handler.request = request


# def _teardown():
#     pass


# @with_setup(_setup, _teardown)
# def test_msg_status():
#     assert_equals(reconnect_handler._msg_status(good_msg), 'ok')
#     assert_equals(reconnect_handler._msg_status(bad_msg), 'error')


# @with_setup(_setup, _teardown)
# def test_msg_successful():
#     assert_equals(reconnect_handler._msg_successful(good_msg), True)
#     assert_equals(reconnect_handler._msg_successful(bad_msg), False)


# @with_setup(_setup, _teardown)
# def test_msg_error():
#     assert_equals(reconnect_handler._msg_error(good_msg), None)
#     assert_equals(reconnect_handler._msg_error(bad_msg), u'{}:\n{}'.format('SyntaxError', 'oh no!'))


# @with_setup(_setup, _teardown)
# def test_get_kernel_manager_no_existing_kernel():
#     kernel_name = "kernel"
#     reconnect_handler._get_kernel_manager('not_existing_path.ipynb', kernel_name)
    
#     session_manager.create_session.assert_called_once_with(kernel_name=kernel_name, path=path)
#     kernel_manager.get_kernel.assert_called_once_with(kernel_id)


# @with_setup(_setup, _teardown)
# def test_post_no_json():
#     reconnect_handler.request.body = "{{}"

#     assert reconnect_handler.post() is None

#     msg = "Invalid JSON in request body."
#     reconnect_handler.set_status.assert_called_once_with(400)
#     reconnect_handler.finish.assert_called_once_with(msg)
#     spark_events.emit_cluster_change_event.assert_called_once_with(None, 400, False, msg)


# @with_setup(_setup, _teardown)
# def test_post_no_key():
#     reconnect_handler.request.body = json.dumps({})

#     assert reconnect_handler.post() is None

#     msg = 'HTTP 400: Bad Request (Missing argument path)'
#     reconnect_handler.set_status.assert_called_once_with(400)
#     reconnect_handler.finish.assert_called_once_with(msg)
#     spark_events.emit_cluster_change_event.assert_called_once_with(None, 400, False, msg)


# @with_setup(_setup, _teardown)
# def test_post_non_existing_kernel():
#     reconnect_handler._get_kernel_manager = MagicMock(return_value=None)

#     assert reconnect_handler.post() is None

#     reconnect_handler.set_status.assert_called_once_with(404)
#     reconnect_handler.finish.assert_called_once_with('{"error": "No kernel for given path", "success": false}')
#     spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 404, False, "No kernel for given path")


# @with_setup(_setup, _teardown)
# def test_post_existing_kernel():
#     assert reconnect_handler.post() is None

#     individual_kernel_manager.restart_kernel.assert_called_once_with()
#     code = '%{} -s {} -u {} -p {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password)
#     client.execute.assert_called_once_with(code, silent=False, store_history=False)
#     reconnect_handler.set_status.assert_called_once_with(200)
#     reconnect_handler.finish.assert_called_once_with('{"error": null, "success": true}')
#     spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 200, True, None)


# @with_setup(_setup, _teardown)
# def test_post_existing_kernel_failed():
#     client.get_shell_msg = MagicMock(return_value=bad_msg)

#     assert reconnect_handler.post() is None

#     individual_kernel_manager.restart_kernel.assert_called_once_with()
#     code = '%{} -s {} -u {} -p {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password)
#     client.execute.assert_called_once_with(code, silent=False, store_history=False)
#     reconnect_handler.set_status.assert_called_once_with(500)
#     reconnect_handler.finish.assert_called_once_with('{"error": "SyntaxError:\\noh no!", "success": false}')
#     spark_events.emit_cluster_change_event.assert_called_once_with(endpoint, 500, False, "SyntaxError:\noh no!")
