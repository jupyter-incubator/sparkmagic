# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException

import sparkmagic.utils.configuration as conf

import pandas as pd

class SendPandasDfToSparkCommand(SendToSparkCommand):

    # convert unicode to utf8 or pyspark will mark data as corrupted(and deserialize incorrectly)
    _python_decode = u'''
        import sys
        import json

        if sys.version_info.major == 2:
            def json_loads_byteified(json_text):
                return _byteify(
                    json.loads(json_text, object_hook=_byteify),
                    ignore_dicts=True
                )
        else:
            def json_loads_byteified(json_text):
                return json.loads(json_text)

        def _byteify(data, ignore_dicts = False):
            if isinstance(data, unicode):
                return data.encode('utf-8')
            if isinstance(data, list):
                return [ _byteify(item, ignore_dicts=True) for item in data ]
            if isinstance(data, dict) and not ignore_dicts:
                return {
                    _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
                    for key, value in data.iteritems()
                }
            return data
    '''

    def __init__(self, input_variable_name, input_variable_value, output_variable_name, max_rows):
        super(SendPandasDfToSparkCommand, self).__init__(input_variable_name, input_variable_value, output_variable_name)
        self.max_rows = max_rows

    def _scala_command(self, input_variable_name, pandas_df, output_variable_name):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)
        pandas_json = self._get_dataframe_as_json(pandas_df)

        scala_code = u'''
        val rdd_json_array = spark.sparkContext.makeRDD("""{}""" :: Nil)
        val {} = spark.read.json(rdd_json_array)'''.format(pandas_json, output_variable_name)

        return Command(scala_code)

    def _pyspark_command(self, input_variable_name, pandas_df, output_variable_name):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)

        pyspark_code = self._python_decode

        pandas_json = self._get_dataframe_as_json(pandas_df)

        pyspark_code += u'''
        json_array = json_loads_byteified('{}')
        rdd_json_array = spark.sparkContext.parallelize(json_array)
        {} = spark.read.json(rdd_json_array)'''.format(pandas_json, output_variable_name)

        return Command(pyspark_code)

    def _r_command(self, input_variable_name, pandas_df, output_variable_name):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)
        pandas_json = self._get_dataframe_as_json(pandas_df)

        r_code = u'''
        fileConn<-file("temporary_pandas_df_sparkmagics.txt")
        writeLines('{}', fileConn)
        close(fileConn)
        {} <- read.json("temporary_pandas_df_sparkmagics.txt")
        {}.persist()
        file.remove("temporary_pandas_df_sparkmagics.txt")'''.format(pandas_json, output_variable_name, output_variable_name)

        return Command(r_code)

    def _get_dataframe_as_json(self, pandas_df):
        return pandas_df.head(self.max_rows).to_json(orient=u'records')

    def _assert_input_is_pandas_dataframe(self, input_variable_name, input_variable_value):
        if not isinstance(input_variable_value, pd.DataFrame):
            wrong_type = input_variable_value.__class__.__name__
            raise BadUserDataException(u'{} is not a Pandas DataFrame! Got {} instead.'.format(input_variable_name, wrong_type))
