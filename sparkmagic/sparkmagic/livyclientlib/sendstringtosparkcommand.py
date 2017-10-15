from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command


class SendStringToSparkCommand(SendToSparkCommand):

    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        unicode_str = unicode(input_variable_value)
        code_to_execute = 'var {} = {}'.format(output_variable_name, unicode_str)
        raise Command(code_to_execute)

    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name, encode_result=True):
        unicode_str = unicode(input_variable_value)
        code_to_execute = '{} = {}'.format(output_variable_name, unicode_str)
        return Command(code_to_execute)

    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        unicode_str = unicode(input_variable_value)
        code_to_execute = 'assign("{}",{})'.format(output_variable_name, unicode_str)
        raise Command(code_to_execute)