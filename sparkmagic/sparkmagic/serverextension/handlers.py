import json
from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler
from tornado import web
from tornado import gen
from tornado.web import MissingArgumentError
from tornado.escape import json_decode

from sparkmagic.kernels.kernelmagics import KernelMagics
import sparkmagic.utils.configuration as conf
from sparkmagic.utils import constants
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.sparklogger import SparkLog


class ReconnectHandler(IPythonHandler):
    logger = None

    @web.authenticated
    @gen.coroutine
    def post(self):
        self.logger = SparkLog(u"ReconnectHandler")

        spark_events = self._get_spark_events()

        try:
            data = json_decode(self.request.body)
        except ValueError as e:
            self.set_status(400)
            msg = "Invalid JSON in request body."
            self.logger.error(msg)
            self.finish(msg)
            spark_events.emit_cluster_change_event(None, 400, False, msg)
            return

        endpoint = None
        try:
            path = self._get_argument_or_raise(data, 'path')
            username = self._get_argument_or_raise(data, 'username')
            password = self._get_argument_or_raise(data, 'password')
            endpoint = self._get_argument_or_raise(data, 'endpoint')
            auth = self._get_argument_if_exists(data, 'auth')
            if auth is None:
                if username == '' and password == '':
                    auth = constants.NO_AUTH
                else:
                    auth = constants.AUTH_BASIC
        except MissingArgumentError as e:
            self.set_status(400)
            self.finish(str(e))
            self.logger.error(str(e))
            spark_events.emit_cluster_change_event(endpoint, 400, False, str(e))
            return

        kernel_name = self._get_kernel_name(data)

        # Get kernel manager, create a new kernel if none exists or restart the existing one when applicable
        kernel_manager = yield self._get_kernel_manager(path, kernel_name)

        # Execute code
        client = kernel_manager.client()
        code = '%{} -s {} -u {} -p {} -t {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password, auth)
        response_id = client.execute(code, silent=False, store_history=False)
        msg = client.get_shell_msg(response_id)

        # Get execution info
        successful_message = self._msg_successful(msg)
        error = self._msg_error(msg)
        if successful_message:
            status_code = 200
        else:
            self.logger.error(u"Code to reconnect errored out: {}".format(error))
            status_code = 500

        # Post execution info
        self.set_status(status_code)
        self.finish(json.dumps(dict(success=successful_message, error=error), sort_keys=True))
        spark_events.emit_cluster_change_event(endpoint, status_code, successful_message, error)

    def _get_kernel_name(self, data):
        kernel_name = self._get_argument_if_exists(data, 'kernelname')
        self.logger.debug("Kernel name is {}".format(kernel_name))
        if kernel_name is None:
            kernel_name = conf.server_extension_default_kernel_name()
            self.logger.debug("Defaulting to kernel name {}".format(kernel_name))
        return kernel_name

    def _get_argument_if_exists(self, data, key):
        return data.get(key)

    def _get_argument_or_raise(self, data, key):
        try:
            return data[key]
        except KeyError:
            raise MissingArgumentError(key)

    @gen.coroutine
    def _get_kernel_manager(self, path, kernel_name):
        sessions = self.session_manager.list_sessions()

        kernel_id = None
        for session in sessions:
            if session['notebook']['path'] == path:
                session_id = session['id']
                kernel_id = session['kernel']['id']
                existing_kernel_name = session['kernel']['name']
                break

        if kernel_id is None:
            self.logger.debug(u"Kernel not found. Starting a new kernel.")
            k_m = yield self._get_kernel_manager_new_session(path, kernel_name)
        elif existing_kernel_name != kernel_name:
            self.logger.debug(u"Existing kernel name '{}' does not match requested '{}'. Starting a new kernel.".format(existing_kernel_name, kernel_name))
            self._delete_session(session_id)
            k_m = yield self._get_kernel_manager_new_session(path, kernel_name)
        else:
            self.logger.debug(u"Kernel found. Restarting kernel.")
            k_m = self.kernel_manager.get_kernel(kernel_id)
            k_m.restart_kernel()

        raise gen.Return(k_m)

    @gen.coroutine
    def _get_kernel_manager_new_session(self, path, kernel_name):
        model_future = self.session_manager.create_session(kernel_name=kernel_name, path=path, type="notebook")
        model = yield model_future
        kernel_id = model["kernel"]["id"]
        self.logger.debug("Kernel created with id {}".format(str(kernel_id)))
        k_m = self.kernel_manager.get_kernel(kernel_id)
        raise gen.Return(k_m)

    def _delete_session(self, session_id):
        self.session_manager.delete_session(session_id)

    def _msg_status(self, msg):
        return msg['content']['status']

    def _msg_successful(self, msg):
        return self._msg_status(msg) == 'ok'

    def _msg_error(self, msg):
        if self._msg_status(msg) != 'error':
            return None
        return u'{}:\n{}'.format(msg['content']['ename'], msg['content']['evalue'])

    def _get_spark_events(self):
        spark_events = getattr(self, 'spark_events', None)
        if spark_events is None:
            return SparkEvents()
        return spark_events


def load_jupyter_server_extension(nb_app):
    nb_app.log.info("sparkmagic extension enabled!")
    web_app = nb_app.web_app

    base_url = web_app.settings['base_url']
    host_pattern = '.*$'

    route_pattern_reconnect = url_path_join(base_url, '/reconnectsparkmagic')
    handlers = [(route_pattern_reconnect, ReconnectHandler)]

    web_app.add_handlers(host_pattern, handlers)
