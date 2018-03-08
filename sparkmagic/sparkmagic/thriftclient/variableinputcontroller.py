from __future__ import print_function
from ipywidgets import widgets, Layout
from collections import namedtuple
from IPython.display import display

from sparkmagic.thriftclient.thriftutils import DefaultVar, NamedVar
from . closablewidget import ClosableWidget

class VaribleInputController(ClosableWidget):
    ROW_WIDTH = 800
    TEXT_WIDTH = 500
    LABEL_WIDTH = ROW_WIDTH - TEXT_WIDTH
    HEIGHT = 30
    px = lambda x: '{}px'.format(x)

    layout_row = Layout(width=px(ROW_WIDTH), max_width=px(ROW_WIDTH), height=px(HEIGHT), max_height=px(HEIGHT),border='solid')
    layout_text = Layout(width=px(TEXT_WIDTH), max_width=px(TEXT_WIDTH), height=px(HEIGHT), max_height=px(HEIGHT),border='solid')
    layout_label = Layout(width=px(LABEL_WIDTH), max_width=px(LABEL_WIDTH), height=px(HEIGHT), max_height=px(HEIGHT),border='solid')

    _TextLabel = namedtuple('TextLabel', ['text_widget', 'label_widget'])

    def __init__(self, callback=None, as_dict=False, delete_widget_on_update=False, **kwargs):
        super(VaribleInputController, self).__init__(**kwargs)

        self.text_widets = None

        self.template_boxes = []
        self.texts_labels = []

        self.container_box = None

        self._as_dict = as_dict
        self._delete_widget_on_update = delete_widget_on_update
        self.callback = callback

    # Useful when we need to pass object into callback
    # Must be set before adding variables
    def set_callback(callback):
        self.callback = callback

    def _pressed_enter(self, *args):
        namedvars = []
        for textlabel in self.texts_labels:
            text_input = textlabel.text_widget.value if textlabel.text_widget.value else textlabel.text_widget.placeholder
            namedvars.append(NamedVar(name=textlabel.label_widget.value, value=text_input))

        self.callback(namedvars)
        if self._delete_widget_on_update:
            self.container_box.close()


    def _pressed_enter_dict(self, *args):
        namedvars = {}
        for textlabel in self.texts_labels:
            text_input = textlabel.text_widget.value if textlabel.text_widget.value else textlabel.text_widget.placeholder
            namedvars[textlabel.label_widget.value] = text_input

        self.callback(namedvars)
        if self._delete_widget_on_update:
            self.container_box.close()

    def addvariable(self, defaultvar):
        if not self.callback:
            raise VariableInputControllerError("Must set callback function before adding variables")
        wid_label = widgets.Label(defaultvar.varible, layout=self.layout_label)
        wid_text = widgets.Text(placeholder=defaultvar.default, continuous_update=True, layout=self.layout_text)
        box_row = widgets.VBox(children=(wid_label, wid_text), layout=self.layout_row)

        if not self._as_dict:
            wid_text.on_submit(self._pressed_enter)
        else:
            wid_text.on_submit(self._pressed_enter_dict)

        self.texts_labels.append(self._TextLabel(text_widget=wid_text, label_widget=wid_label))
        self.template_boxes.append(box_row)

    def _create_container_box(self):
        self.container_box = widgets.VBox(children=tuple(self.template_boxes))

    def display_all(self):
        if self.container_box is None:
            self._create_container_box()
        display(self.container_box)

class VariableInputControllerError(Exception):
    pass
