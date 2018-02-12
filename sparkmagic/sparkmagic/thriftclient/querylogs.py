class QueryLogs:
    def __init__(self):
        self._logs = []

    def __add__(self, log):
        self.add(log)

    def clear(self):
        del self._logs[:]

    def add(self, log):
        if type(log) is list:
            self._logs += log
        elif type(log) is tuple:
            self._logs += list(log)
        elif type(log) is str:
            self._logs += [log]
        else:
            self._logs += [str(log)]

    def __str__(self):
        return "\n".join(self._logs)
