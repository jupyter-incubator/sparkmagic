class ThriftExecutionError(Exception):
    def __init__(self, m):
        self.message = m

class ThriftMissingKeywordError(Exception):
    def __init__(self, m):
        self.message = m

class ThriftConfigurationError(Exception):
    def __init__(self, m):
        self.message = m
