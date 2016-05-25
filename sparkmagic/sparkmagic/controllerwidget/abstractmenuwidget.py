from ipywidgets import Box

from hdijupyterutils.ipythondisplay import IpythonDisplay
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory


class AbstractMenuWidget(Box):
    def __init__(self, spark_controller, ipywidget_factory=None, ipython_display=None,
                 nested_widget_mode=False, testing=False, **kwargs):
        kwargs['orientation'] = 'vertical'

        if not testing:
            super(AbstractMenuWidget, self).__init__((), **kwargs)

        self.spark_controller = spark_controller

        if ipywidget_factory is None:
            ipywidget_factory = IpyWidgetFactory()
        self.ipywidget_factory = ipywidget_factory

        if ipython_display is None:
            ipython_display = IpythonDisplay()
        self.ipython_display = ipython_display

        self.children = []

        if not nested_widget_mode:
            self._repr_html_()

    def _repr_html_(self):
        for child in self.children:
            self.ipython_display.display(child)
        return ""

    def hide_all(self):
        for child in self.children:
            child.visible = False

    def run(self):
        raise NotImplementedError("Concrete menu widget must define run")
