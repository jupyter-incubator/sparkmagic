class OutputHandler:
    def __init__(self, callback_clear, ndisplay=-1):
        self._displayed = []
        self._ndisplay = ndisplay
        self._callback_clear = callback_clear

    def __add__(self, log):
        self._add(log)

    def clear(self):
        del self._displayed[:]

    def clear_display(self):
        self._callback_clear()

    def _add(self, log):
        if type(log) is list:
            extra = self.splitonnewlines(log)
        elif type(log) is tuple:
            extra = self.splitonnewlines(list(log))
        elif type(log) is str:
            extra = self.splitonnewlines([log])
        else:
            extra = self.splitonnewlines([str(log)])
        self._displayed += extra
        return extra

    def _display(self, callback, *args):
        extra = self + args
        refresh = self._ndisplay > -1 and len(self) > self._ndisplay
        if refresh and self._callback_clear:
            self._callback_clear()
            callback(str(self))
        else:
            callback(*args)

    def fulllog(self):
        return "\n".join(self._displayed)

    def splitonnewlines(self, nlist):
        return sum([str(log).split('\n') for log in nlist], [])

    def __str__(self):
        return "\n".join(self._displayed[-self._ndisplay:])

    def __len__(self):
        return len(self._displayed)

    def __getitem__(self, item):
        if isinstance(item, slice):
            indices = item.indices(len(self))
            return self._displayed.__getitem__(slice(*indices))
        else:
            return self._displayed[item]

    def wrap(self, callback):
        def _writeln(*args):
            self._display(callback, *args)
        return _writeln
