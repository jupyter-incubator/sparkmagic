from collections import namedtuple

class BashResult(namedtuple('BashResult', 'stdout stderr')):
    __slots__ = ()

    def __str__(self):
        return 'STDOUT:\n{}STDERR:\n{}'.format(self.stdout, self.stderr)

    def __repr__(self):
        return '{!r}(out={!r},err={!r})' % (self.__class__, self.stdout, self.stderr)
