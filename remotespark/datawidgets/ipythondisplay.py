from IPython.core.display import display, HTML


class IpythonDisplay(object):
    @staticmethod
    def display_to_ipython(to_display):
        display(to_display)

    @staticmethod
    def html_to_ipython(to_display):
        IpythonDisplay.display_to_ipython(HTML(to_display))
