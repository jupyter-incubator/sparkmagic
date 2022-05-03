from ipywidgets.widgets import FloatProgress, Layout

class ProgressIndicator:
    def __init__(self, session, statement_id):
        pass

    def display(self):
        pass

    def update(self, value):
        pass

    def close(self):
        pass

class HorizontalFloatProgressWidgetIndicator(ProgressIndicator):
    def __init__(self, session, statement_id):
        self.session = session
        self.statement_id = statement_id
        self.progress = FloatProgress(value=0.0,
                                 min=0,
                                 max=1.0,
                                 step=0.01,
                                 description='Progress:',
                                 bar_style='info',
                                 orientation='horizontal',
                                 layout=Layout(width='50%', height='25px')
        )

    def display(self):
        self.session.ipython_display.display(self.progress)

    def update(self, value):
        self.progress.value = value

    def close(self):
        self.progress.close()
