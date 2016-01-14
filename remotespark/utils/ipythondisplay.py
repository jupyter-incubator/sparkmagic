from IPython.core.display import display, HTML
from IPython import get_ipython


class IpythonDisplay(object):
    def __init__(self):
        self._ipython_shell = get_ipython()

    def display(self, to_display):
        display(to_display)

    def html(self, to_display):
        IpythonDisplay.display(HTML(to_display))

    def write(self, msg):
        self._ipython_shell.write(msg)

    def writeln(self, msg):
        self.write("{}\n".format(msg))

    def send_error(self, error):
        self._ipython_shell.write_err(error)
