from sparkmagic.livyclientlib.command import Command

from sparkmagic.livyclientlib.exceptions import BadUserDataException


class SendToSparkCommand(Command):
    def __init__(self, output_var_name, output_var_value, spark_events=None):
        super(SendToSparkCommand, self).__init__("", spark_events)
        self.output_var_name = output_var_name
        self.output_var_value = output_var_value

    def execute(self, session):
        try:
            command = self.to_command(session.kind, self.output_var_name, self.output_var_value)
            (success, result) = command.execute(session)
            result = self.output_var_value #todo ISSUE#412
            if not success:
                raise BadUserDataException()
        except Exception as e:
            raise
        else:
            return (success, result)


    def _scala_command(self, spark_context_variable_name, local_context_variable_value):
        return Command(u'println("Scala Command")')


    def _pyspark_command(self, spark_context_variable_name, local_context_variable_value, encode_result=True):
        return Command(u'print("Python Command")')


    def _r_command(self, spark_context_variable_name, local_context_variable_value):
        return Command(u'sprintf("R Command");')

