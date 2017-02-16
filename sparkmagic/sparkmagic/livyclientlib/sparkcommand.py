from sparkmagic.livyclientlib.command import Command

class SparkStoreCommand(Command):
    def __init__(self, code, samplemethod, maxrows, samplefraction, output_var, spark_events=None):
        super(SparkStoreCommand, self).__init__(code, spark_events)
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
            command = self._to_command(session.kind, output_var)
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
            return self._pyspark_command(spark_context_variable_name, False)
        elif kind == constants.SESSION_KIND_SPARK:
            return self._scala_command(spark_context_variable_name)
        elif kind == constants.SESSION_KIND_SPARKR:
            return self._r_command(spark_context_variable_name)
        else:
            raise BadUserDataException(u"Kind '{}' is not supported.".format(kind))

    def _pyspark_command(spark_context_variable_name):
        command = u'{}.toJSON()'.format(spark_context_variable_name)
        if self.samplemethod == u'sample':
            command = u'{}.sample(False, {})'.format(command, self.samplefraction)
        if self.maxrows >= 0:
            command = u'{}.take({})'.format(command, self.maxrows)
        else:
            command = u'{}.collect()'.format(command)
        #what about encoding?
        return Command(command)

    def _scala_command(spark_context_variable_name):
        #TODO add functionality
        command = u'{}.toJSON'.format(spark_context_variable_name)
        return Command(u'{}.foreach(println)'.format(command))

    def _r_command(spark_context_variable_name):
        #TODO implement
        pass


    @staticmethod
    def _records_to_dataframe(records_text, kind):
        #TODO currently, duplicate in sqlquery.py, move both to command.py?
        if records_text == '':
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