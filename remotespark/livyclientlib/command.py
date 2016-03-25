import textwrap

from remotespark.utils.guid import ObjectWithGuid
from remotespark.utils.log import Log
from remotespark.utils.sparkevents import SparkEvents

class Command(ObjectWithGuid):
    def __init__(self, code):
        super(Command, self).__init__()
        self.code = textwrap.dedent(code)
        self.logger = Log("Command")
        self._spark_events = SparkEvents()

    def __eq__(self, other):
        return self.code == other.code

    def __ne__(self, other):
        return not self == other

    def execute(self, session):
        self._spark_events.emit_statement_execution_start_event(session.guid, session.kind, session.id, self.guid)
        statement_id = -1
        try:
            session.wait_for_idle()
            data = {"code": self.code}
            response = session.http_client.post_statement(session.id, data)
            statement_id = response['id']
            output = self._get_statement_output(session, statement_id)
        except Exception as e:
            self._spark_events.emit_statement_execution_end_event(session.guid, session.kind, session.id,
                                                                  self.guid, statement_id, False, str(type(e)),
                                                                  str(e))
            raise
        else:
            self._spark_events.emit_statement_execution_end_event(session.guid, session.kind, session.id,
                                                                  self.guid, statement_id, True, "", "")
            return output

    def _get_statement_output(self, session, statement_id):
        statement_running = True
        out = ""
        while statement_running:
            statement = session.http_client.get_statement(session.id, statement_id)
            status = statement["state"]

            self.logger.debug("Status of statement {} is {}.".format(statement_id, status))

            if status == "running":
                session.sleep()
            else:
                statement_running = False

                statement_output = statement["output"]
                if statement_output["status"] == "ok":
                    out = (True, statement_output["data"]["text/plain"])
                elif statement_output["status"] == "error":
                    out = (False,
                           statement_output["evalue"] + "\n" + "".join(statement_output["traceback"]))
                else:
                    raise ValueError("Unknown output status: '{}'".format(statement_output["status"]))

        return out
