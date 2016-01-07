"""An exception that is thrown by the Livy client when it can't parse a dataframe
(i.e. when the query resulted in an error"""

class DataFrameParseException(Exception):
    def __init__(self, out=None):
        self.out = out
