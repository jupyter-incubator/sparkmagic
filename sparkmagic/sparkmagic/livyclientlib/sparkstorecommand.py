# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from sparkmagic.utils.utils import records_to_dataframe
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import DataFrameParseException, BadUserDataException
import sparkmagic.utils.constants as constants

import ast

class SparkStoreCommand(Command):
    def __init__(self, output_var, samplemethod=None, maxrows=None, samplefraction=None, spark_events=None, coerce=None):
        super(SparkStoreCommand, self).__init__("", spark_events)

        if samplemethod is None:
            samplemethod = conf.default_samplemethod()
        if maxrows is None:
            maxrows = conf.default_maxrows()
        if samplefraction is None:
            samplefraction = conf.default_samplefraction()

        if samplemethod not in {u'take', u'sample'}:
            raise BadUserDataException(u'samplemethod (-m) must be one of (take, sample)')
        if not isinstance(maxrows, int):
            raise BadUserDataException(u'maxrows (-n) must be an integer')
        if not 0.0 <= samplefraction <= 1.0:
            raise BadUserDataException(u'samplefraction (-r) must be a float between 0.0 and 1.0')

        self.samplemethod = samplemethod
        self.maxrows = maxrows
        self.samplefraction = samplefraction
        self.output_var = output_var
        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events
        self._coerce = coerce


    def execute(self, session):
        try:
            command = self.to_command(session.kind, self.output_var)
            (success, records_text, mimetype) = command.execute(session)
            if not success:
                raise BadUserDataException(records_text)
            result = records_to_dataframe(records_text, session.kind, self._coerce)
        except Exception as e:
            raise
        else:
            return result


    def to_command(self, kind, spark_context_variable_name):
        if kind == constants.SESSION_KIND_PYSPARK:
            return self._pyspark_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_SPARK:
            return self._scala_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_SPARKR:
            return self._r_command(spark_context_variable_name)
        else:
            raise BadUserDataException(u"Kind '{}' is not supported.".format(kind))


    def _pyspark_command(self, spark_context_variable_name):
        # use_unicode=False means the result will be UTF-8-encoded bytes, so we
        # set it to False for Python 2.
        command = u'{}.toJSON(use_unicode=(sys.version_info.major > 2))'.format(
            spark_context_variable_name)
        if self.samplemethod == u'sample':
            command = u'{}.sample(False, {})'.format(command, self.samplefraction)
        if self.maxrows >= 0:
            command = u'{}.take({})'.format(command, self.maxrows)
        else:
            command = u'{}.collect()'.format(command)
        # Unicode support has improved in Python 3 so we don't need to encode.
        print_command = constants.LONG_RANDOM_VARIABLE_NAME
        command = u'import sys\nfor {} in {}: print({})'.format(
            constants.LONG_RANDOM_VARIABLE_NAME,
            command,
            print_command)
        return Command(command)


    def _scala_command(self, spark_context_variable_name):
        command = u'{}.toJSON'.format(spark_context_variable_name)
        if self.samplemethod == u'sample':
            command = u'{}.sample(false, {})'.format(command, self.samplefraction)
        if self.maxrows >= 0:
            command = u'{}.take({})'.format(command, self.maxrows)
        else:
            command = u'{}.collect'.format(command)
        return Command(u'{}.foreach(println)'.format(command))


    def _r_command(self, spark_context_variable_name):
        command = spark_context_variable_name
        if self.samplemethod == u'sample':
            command = u'sample({}, FALSE, {})'.format(command,
                                                      self.samplefraction)
        if self.maxrows >= 0:
            command = u'take({},{})'.format(command, self.maxrows)
        else:
            command = u'collect({})'.format(command)
        command = u'jsonlite::toJSON({})'.format(command)
        command = u'for ({} in ({})) {{cat({})}}'.format(constants.LONG_RANDOM_VARIABLE_NAME,
                                                         command,
                                                         constants.LONG_RANDOM_VARIABLE_NAME)
        return Command(command)


    # Used only for unit testing
    def __eq__(self, other):
        return self.code == other.code and \
            self.samplemethod == other.samplemethod and \
            self.maxrows == other.maxrows and \
            self.samplefraction == other.samplefraction and \
            self.output_var == other.output_var and \
            self._coerce == other._coerce

    def __ne__(self, other):
        return not (self == other)
