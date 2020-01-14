# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException

class SendStringToSparkCommand(SendToSparkCommand):

    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        scala_code = u'var {} = """{}"""'.format(output_variable_name, input_variable_value)
        return Command(scala_code)

    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        pyspark_code = u'{} = {}'.format(output_variable_name, repr(input_variable_value))
        return Command(pyspark_code)

    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        escaped_input_variable_value = input_variable_value.replace(u'\\', u'\\\\').replace(u'"',u'\\"')
        r_code = u'''assign("{}","{}")'''.format(output_variable_name, escaped_input_variable_value)
        return Command(r_code)

    def _assert_input_is_string_type(self, input_variable_name, input_variable_value):
        if not isinstance(input_variable_value, str):
            wrong_type = input_variable_value.__class__.__name__
            raise BadUserDataException(u'{} is not a str or bytes! Got {} instead'.format(input_variable_name, wrong_type))
