from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command


class SendPandasDfToSparkCommand(SendToSparkCommand):

    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        raise NotImplementedError


    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name, encode_result=True):
        raise NotImplementedError


    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        raise NotImplementedError