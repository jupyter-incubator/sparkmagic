from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command


class SendStringToSparkCommand(SendToSparkCommand):

    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        code_to_execute = 'var {} = {}'.format(input_variable_name, input_variable_value)
        raise NotImplementedError


    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name, encode_result=True):
        code_to_execute = '{} = {}'.format(output_variable_name, input_variable_value)
        return Command(code_to_execute)


    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        raise NotImplementedError
