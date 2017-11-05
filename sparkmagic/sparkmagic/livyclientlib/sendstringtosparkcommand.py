from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException

import re

class SendStringToSparkCommand(SendToSparkCommand):

    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        code_to_execute = u'var {} = """{}"""'.format(output_variable_name, input_variable_value)
        return Command(code_to_execute)

    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name, python2):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        code_to_execute = u'{} = \'\'\'{}\'\'\''.format(output_variable_name, input_variable_value)
        return Command(code_to_execute)

    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        self._assert_input_is_string_type(input_variable_name, input_variable_value)
        escaped_input_variable_value = re.escape(input_variable_value)
        code_to_execute = u'assign("{}","{}")'.format(output_variable_  name, escaped_input_variable_value)
        return Command(code_to_execute)

    def _assert_input_is_string_type(self, input_variable_name, input_variable_value):
        if not isinstance(input_variable_value, str):
            raise BadUserDataException(u'{} is not a String!'.format(input_variable_name))