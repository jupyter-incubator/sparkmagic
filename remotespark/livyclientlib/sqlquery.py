import remotespark.utils.configuration as conf
import remotespark.utils.constants as constants


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

    def to_command(self, kind):
        if kind == constants.SESSION_KIND_PYSPARK:
            return self._pyspark_command()
        elif kind == constants.SESSION_KIND_SPARK:
            return self._scala_command()
        elif kind == constants.SESSION_KIND_SPARKR:
            return self._r_command()
        else:
            raise ValueError("Kind '{}' is not supported.".format(kind))

    def _pyspark_command(self):
        command = 'sqlContext.sql("""{}""")'.format(self.query)
        if self.only_columns:
            command = '{}.columns'.format(command)
        else:
            command = '{}.toJSON()'.format(command)
            if self.samplemethod == 'sample':
                command = '{}.sample(False, {})'.format(command, self.samplefraction)
            if self.maxrows >= 0:
                command = '{}.take({})'.format(command, self.maxrows)
            else:
                command = '{}.collect()'.format(command)
        command = 'for {} in {}: print({})'.format(constants.LONG_RANDOM_VARIABLE_NAME,
                                                   command,
                                                   constants.LONG_RANDOM_VARIABLE_NAME)
        return command

    def _scala_command(self):
        command = 'sqlContext.sql("""{}""")'.format(self.query)
        if self.only_columns:
            command = '{}.columns'.format(command)
        else:
            command = '{}.toJSON'.format(command)
            if self.samplemethod == 'sample':
                command = '{}.sample(false, {})'.format(command, self.samplefraction)
            if self.maxrows >= 0:
                command = '{}.take({})'.format(command, self.maxrows)
            else:
                command = '{}.collect'.format(command)
        return '{}.foreach(println)'.format(command)

    def _r_command(self):
        raise NotImplementedError()

    @staticmethod
    def as_only_columns_query(query):
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