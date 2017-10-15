from sparkmagic.livyclientlib.sendtosparkcommand import SendToSparkCommand
from sparkmagic.livyclientlib.command import Command


class SendStringToSparkCommand(SendToSparkCommand):

    def _scala_command(self, spark_context_variable_name, local_context_variable_value):
        raise NotImplementedError


    def _pyspark_command(self, spark_context_variable_name, local_context_variable_value, encode_result=True):
        code_to_execute = '{} = {}'.format(spark_context_variable_name, local_context_variable_value)
        return Command(code_to_execute)


    def _r_command(self, spark_context_variable_name, local_context_variable_value):
        raise NotImplementedError
