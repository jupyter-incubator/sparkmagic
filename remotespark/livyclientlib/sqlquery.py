import remotespark.utils.configuration as conf


class SQLQuery(object):
    def __init__(self, query, samplemethod=None, maxrows=None,
                 samplefraction=None, only_columns=False):
        if samplemethod is None:
            samplemethod = conf.default_samplemethod()
        if maxrows is None:
            maxrows = conf.default_maxrows()
        if samplefraction is None:
            samplefraction = conf.default_samplefraction()

        assert samplemethod == 'take' or samplemethod == 'sample'
        assert isinstance(maxrows, int)
        assert 0.0 <= samplefraction <= 1.0

        self.query = query
        self.samplemethod = samplemethod
        self.maxrows = maxrows
        self.samplefraction = samplefraction
        self.only_columns = only_columns

    @staticmethod
    def get_only_columns_query(query):
        """Given a SQL query, return a new version of that SQL query which only gets
        the columns for that query."""
        return SQLQuery(query.query, query.samplemethod, query.maxrows,
                        query.samplefraction, True)

    # Used only for unit testing
    def __eq__(self, other):
        return self.query == other.query and \
            self.samplemethod == other.samplemethod and \
            self.maxrows == other.maxrows and \
            self.samplefraction == other.samplefraction and \
            self.only_columns == other.only_columns

    def __ne__(self, other):
        return not (self == other)