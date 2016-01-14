from IPython.core.display import display, HTML
from IPython import get_ipython


class IpythonDisplay(object):
    @staticmethod
    def display(to_display):
        display(to_display)

    @staticmethod
    def html(to_display):
        IpythonDisplay.display(HTML(to_display))

    @staticmethod
    def write(msg):
        get_ipython().write(msg)

    @staticmethod
    def writeln(msg):
        IpythonDisplay.write("{}\n".format(msg))

    @staticmethod
    def send_error(error, send_response, iopub_socket):
        stream_content = {"name": "stderr", "text": error}
        send_response(iopub_socket, "stream", stream_content)
