from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.exceptions import BadUserDataException
import sparkmagic.utils.constants as constants

from abc import abstractmethod

class SendToSparkCommand(Command):
    def __init__(self, input_variable_name, input_variable_value, output_variable_name, spark_events=None):
        super(SendToSparkCommand, self).__init__("", spark_events)
        self.input_variable_name = input_variable_name
        self.input_variable_value = input_variable_value
        self.output_variable_name = output_variable_name

    def execute(self, session):
        try:
            command = self.to_command(session.kind, self.input_variable_name, self.input_variable_value, self.output_variable_name)
            (success, result) = command.execute(session)
            result = self.input_variable_value #todo ISSUE#412
            if not success:
                raise BadUserDataException(result)
        except Exception as e:
            raise
        else:
            return (success, result)

    def to_command(self, kind, input_variable_name, input_variable_value, output_variable_name):
        if kind == constants.SESSION_KIND_PYSPARK:
            return self._pyspark_command(input_variable_name, input_variable_value, output_variable_name, python2 = True)
        elif kind == constants.SESSION_KIND_PYSPARK3:
            return self._pyspark_command(input_variable_name, input_variable_value, output_variable_name, python2 = False)
        elif kind == constants.SESSION_KIND_SPARK:
            return self._scala_command(input_variable_name, input_variable_value, output_variable_name)
        elif kind == constants.SESSION_KIND_SPARKR:
            return self._r_command(input_variable_name, input_variable_value, output_variable_name)
        else:
            raise BadUserDataException(u"Kind '{}' is not supported.".format(kind))

    @abstractmethod
    def _scala_command(self, input_variable_name, input_variable_value, output_variable_name):
        raise NotImplementedError #override and provide proper implementation in supertype!


    @abstractmethod
    def _pyspark_command(self, input_variable_name, input_variable_value, output_variable_name, python2):
        raise NotImplementedError #override and provide proper implementation in supertype!


    @abstractmethod
    def _r_command(self, input_variable_name, input_variable_value, output_variable_name):
        raise NotImplementedError #override and provide proper implementation in supertype!