from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException

import pandas as pd

class SendPandasDfToSparkCommand(SendToSparkCommand):

    # convert unicode to utf8 or pyspark will mark data as corrupted(and deserialize incorrectly)
    _python_2_decode = u'''
        import json

        def json_loads_byteified(json_text):
            return _byteify(
                json.loads(json_text, object_hook=_byteify),
                ignore_dicts=True
            )
        
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

    # just an alias
    _python_3_decode = u'''
        import json
        
        def json_loads_byteified(json_text):
            return json.loads(json_text)
            
    '''

    def _scala_command(self, input_variable_name, pandas_df, output_variable_name):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)
        pandas_json = pandas_df.to_json(orient=u'records')

        code = u'''
        val rdd_json_array = spark.sparkContext.makeRDD("""{}""" :: Nil)
        val {} = spark.read.json(rdd_json_array)'''.format(pandas_json, output_variable_name)

        return Command(code)

    def _pyspark_command(self, input_variable_name, pandas_df, output_variable_name, python2):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)

        if python2:
            code = self._python_2_decode
        else:
            code = self._python_3_decode

        pandas_json = pandas_df.to_json(orient=u'records')

        code += u'''
        json_array = json_loads_byteified('{}')
        rdd_json_array = spark.sparkContext.parallelize(json_array)
        {} = spark.read.json(rdd_json_array)'''.format(pandas_json, output_variable_name)

        return Command(code)

    def _r_command(self, input_variable_name, pandas_df, output_variable_name):
        self._assert_input_is_pandas_dataframe(input_variable_name, pandas_df)
        raise NotImplementedError

    def _assert_input_is_pandas_dataframe(self, input_variable_name, input_variable_value):
        if not isinstance(input_variable_value, pd.DataFrame):
            raise BadUserDataException(u'{} is not a Pandas DataFrame!'.format(input_variable_name))
