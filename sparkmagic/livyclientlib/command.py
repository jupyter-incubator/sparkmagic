import textwrap

from hdijupyterutils.guid import ObjectWithGuid
from hdijupyterutils.log import Log
from hdijupyterutils.sparkevents import SparkEvents

from .exceptions import LivyUnexpectedStatusException


class Command(ObjectWithGuid):
    def __init__(self, code, spark_events=None):
        super(Command, self).__init__()
        self.code = textwrap.dedent(code)
        self.logger = Log(u"Command")
        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events

    def __eq__(self, other):
        return self.code == other.code

    def __ne__(self, other):
        return not self == other

    def execute(self, session):
        self._spark_events.emit_statement_execution_start_event(session.guid, session.kind, session.id, self.guid)
        statement_id = -1
        try:
            session.wait_for_idle()
            data = {u"code": self.code}
            response = session.http_client.post_statement(session.id, data)
            statement_id = response[u'id']
            output = self._get_statement_output(session, statement_id)
        except Exception as e:
            self._spark_events.emit_statement_execution_end_event(session.guid, session.kind, session.id,
                                                                  self.guid, statement_id, False, e.__class__.__name__,
                                                                  str(e))
            raise
        else:
            self._spark_events.emit_statement_execution_end_event(session.guid, session.kind, session.id,
                                                                  self.guid, statement_id, True, "", "")
            return output

    def _get_statement_output(self, session, statement_id):
        statement_running = True
        out = u""
        while statement_running:
            statement = session.http_client.get_statement(session.id, statement_id)
            status = statement[u"state"]

            self.logger.debug(u"Status of statement {} is {}.".format(statement_id, status))

            if status == u"running":
                session.sleep()
            else:
                statement_running = False

                statement_output = statement[u"output"]
                if statement_output[u"status"] == u"ok":
                    out = (True, statement_output[u"data"][u"text/plain"])
                elif statement_output[u"status"] == u"error":
                    out = (False,
                           statement_output[u"evalue"] + u"\n" + u"".join(statement_output[u"traceback"]))
                else:
                    raise LivyUnexpectedStatusException(u"Unknown output status from Livy: '{}'"
                                                        .format(statement_output[u"status"]))

        return out
