class ClosableWidget(object):
    """
    Class to keep track of widgets that are closed on kernal restart or shutdown
    """

    close_widgets = [] # static

    def __init__(self, **kwargs):
        if kwargs.get('keepopen', None):
            return
        ClosableWidget.close_widgets.append(self)
