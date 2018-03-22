from ipywidgets import IntProgress, HTML, HBox, Layout, Box
from IPython.display import display

class PogressBar:
    def __init__(self, minv=-1, maxv=-1, labelname):
        self.labelname = labelname
        self.progress = IntProgress(min=minv, max=maxv, value=minv)

        label = HTML()
        labelbox = Box(children=[label], layout=Layout(min_width='200px'))
        box = HBox(children=[labelbox, progress])

        label.value = u'{name}: {index} / {size}'.format(name='reduce',index=minv, size=maxv)

def update_query_progress(log, has_started):
    import re

    maps = re.findall(r"map = [0-9]*%", log)
    reducers = re.findall(r"reduce = [0-9]*%", log)

def log_progress(sequence, every=None, size=None, name='Items'):


    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = int(size / 200)     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{name}: {index} / ?'.format(
                        name=name,
                        index=index
                    )
                else:
                    progress.value = index
                    label.value = u'{name}: {index} / {size}'.format(
                        name=name,
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = "{name}: {index}".format(
            name=name,
            index=str(index or '?')
        )
