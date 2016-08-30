import json
from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler


class ReconnectHandler(IPythonHandler):
    def get(self):
        path = 'Temp/Untitled3.ipynb'
        
        # Get kernel manager
        kernel_manager = self.get_kernel_manager(path)
        kernel_manager.restart_kernel()

        # Execute code
        client = kernel_manager.client()        
        response_id = client.execute('"hello world"', silent=False, store_history=False)
        msg = client.get_shell_msg(response_id)

        # Get execution info
        successful_message = self.msg_successful(msg)
        error = self.msg_error(msg)
        
        # Post execution info
        self.finish(json.dumps({"success": successful_message, "error": error}))
            
    def get_kernel_manager(self, path):
        sessions = self.session_manager.list_sessions()
        
        kernel_id = None
        for session in sessions:
            if session['notebook']['path'] == path:
                kernel_id = session['kernel']['id']
        
        return self.kernel_manager.get_kernel(kernel_id)
    
    def msg_status(selg, msg):
        return msg['content']['status']

    def msg_successful(self, msg):
        return self.msg_status(msg) == 'ok'

    def msg_error(self, msg):
        if self.msg_status(msg) != 'error':
            return None
        return u'{}:\n{}'.format(msg['content']['ename'], msg['content']['evalue'])


def load_jupyter_server_extension(nb_app):
    nb_app.log.info("sparkmagic extension enabled!")
    web_app = nb_app.web_app
    
    base_url = web_app.settings['base_url']
    host_pattern = '.*$'
    
    route_pattern_reconnect = url_path_join(base_url, '/reconnectsparkmagic')
    handlers = [(route_pattern_reconnect, ReconnectHandler)]
    
    web_app.add_handlers(host_pattern, handlers)
