from sparkmagic.utils.utils import get_sessions_info_html


class StartupInfoDisplay:
    def __init__(self, ipython_display, sessions_info, current_session_id):
        self.ipython_display = ipython_display
        self.sessions_info = sessions_info
        self.current_session_id = current_session_id

    def write_msg(self, msg):
        pass

    def display(self):
        pass


class HTMLTableStartupInfoDisplay(StartupInfoDisplay):
    def display(self):
        self.ipython_display.html(
            get_sessions_info_html(self.sessions_info, self.current_session_id)
        )

    def write_msg(self, msg):
        self.ipython_display.writeln(msg)
