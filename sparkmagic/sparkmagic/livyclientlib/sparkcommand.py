import json
import pandas as pd
from collections import OrderedDict

from sparkmagic.utils.utils import coerce_pandas_df_to_numeric_datetime
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import DataFrameParseException, BadUserDataException

import ast

class SparkStoreCommand(Command):
    def __init__(self, code, samplemethod=None, maxrows=None, samplefraction=None, output_var=None, spark_events=None):
        super(SparkStoreCommand, self).__init__(code, spark_events)

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

    def execute(self, session):
        return self.store_to_context(session)

    def store_to_context(self, session):
        try:
            command = self._to_command(session.kind, self.output_var)
            (success, records_text) = command.execute(session)
            if not success:
                raise BadUserDataException(records_text)
            result = self._records_to_dataframe(records_text, session.kind)
        except Exception as e:
            raise
        else:
            return result

    def _to_command(self, kind, spark_context_variable_name):
        if kind == constants.SESSION_KIND_PYSPARK:
            return self._pyspark_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_PYSPARK3:
            return self._pyspark_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_SPARK:
            return self._scala_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_SPARKR:
            return self._r_command(spark_context_variable_name)
        else:
            raise BadUserDataException(u"Kind '{}' is not supported.".format(kind))

    def _pyspark_command(self, spark_context_variable_name, encode_result=True):
        command = u'{}.toJSON(use_unicode=True)'.format(spark_context_variable_name)
        if self.samplemethod == u'sample':
            command = u'{}.sample(False, {})'.format(command, self.samplefraction)
        if self.maxrows >= 0:
            command = u'{}.take({})'.format(command, self.maxrows)
        else:
            command = u'{}.collect()'.format(command)
        # Unicode support has improved in Python 3 so we don't need to encode.
        if encode_result:
            print_command = '{}.encode("{}")'.format(constants.LONG_RANDOM_VARIABLE_NAME,
                                                     conf.pyspark_sql_encoding())
        else:
            print_command = constants.LONG_RANDOM_VARIABLE_NAME
        command = u'for {} in {}: print({})'.format(constants.LONG_RANDOM_VARIABLE_NAME,
                                                    command,
                                                    print_command)
        return Command(command)

    def _scala_command(self, spark_context_variable_name):
        #TODO add functionality
        command = u'{}.toJSON'.format(spark_context_variable_name)
        return Command(u'{}.foreach(println)'.format(command))

    def _r_command(self, spark_context_variable_name):
        #TODO implement
        pass


    @staticmethod
    def _records_to_dataframe(records_text, kind):
        #TODO currently, duplicate in sqlquery.py, move both to command.py?
        if records_text in ['','[]']:
            strings = []
        else:
            strings = records_text.split('\n')
        try:
            data_array = [json.JSONDecoder(object_pairs_hook=OrderedDict).decode(s) for s in strings]

            if kind == constants.SESSION_KIND_SPARKR and len(data_array) > 0:
                data_array = data_array[0]

            if len(data_array) > 0:
                df = pd.DataFrame(data_array, columns=data_array[0].keys())
            else:
                df = pd.DataFrame(data_array)
            
            coerce_pandas_df_to_numeric_datetime(df)
            return df
        except ValueError:
            raise DataFrameParseException(u"Cannot parse object as JSON: '{}'".format(strings))