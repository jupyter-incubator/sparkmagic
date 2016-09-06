import json
from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler
from tornado import web

from sparkmagic.kernels.kernelmagics import KernelMagics
from sparkmagic.utils.sparkevents import SparkEvents


class ReconnectHandler(IPythonHandler):
    @web.authenticated
    def post(self):
        path, username, password, endpoint = self._get_parsed_arguments()
        spark_events = self._get_spark_events()
        
        # Get kernel manager
        kernel_manager = self._get_kernel_manager(path)
        if kernel_manager is None:
            status_code = 404
            self.set_status(status_code)
            error = "No kernel for given path"
            self.finish(json.dumps(dict(success=False, error=error)))
            spark_events.emit_cluster_change_event(endpoint, status_code, False, error)
            return

        # Restart
        kernel_manager.restart_kernel()

        # Execute code
        client = kernel_manager.client()
        code = '%{} -s {} -u {} -p {}'.format(KernelMagics._do_not_call_change_endpoint.__name__, endpoint, username, password)    
        response_id = client.execute(code, silent=False, store_history=False)
        msg = client.get_shell_msg(response_id)

        # Get execution info
        successful_message = self._msg_successful(msg)
        error = self._msg_error(msg)
        if successful_message:
            status_code = 200
        else:
            status_code = 500
        
        # Post execution info
        self.set_status(status_code)
        self.finish(json.dumps(dict(success=successful_message, error=error)))
        spark_events.emit_cluster_change_event(endpoint, status_code, successful_message, error)

    def _get_parsed_arguments(self):
        path = self.get_body_argument('path')
        username = self.get_body_argument('username')
        password = self.get_body_argument('password')
        endpoint = self.get_body_argument('endpoint')

        return path, username, password, endpoint
            
    def _get_kernel_manager(self, path):
        sessions = self.session_manager.list_sessions()
        
        kernel_id = None
        for session in sessions:
            if session['notebook']['path'] == path:
                kernel_id = session['kernel']['id']
                break

        if kernel_id is None:
            return None
        
        return self.kernel_manager.get_kernel(kernel_id)
    
    def _msg_status(selg, msg):
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
