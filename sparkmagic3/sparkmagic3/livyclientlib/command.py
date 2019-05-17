import textwrap

from hdijupyterutils.guid import ObjectWithGuid

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME, FINAL_STATEMENT_STATUS
from .exceptions import LivyUnexpectedStatusException


class Command(ObjectWithGuid):
    def __init__(self, code, spark_events=None):
        super(Command, self).__init__()
        self.code = textwrap.dedent(code)
        self.logger = SparkLog(u"Command")
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
        retries = 1
        
        while True:
            statement = session.http_client.get_statement(session.id, statement_id)
            status = statement[u"state"].lower()

            self.logger.debug(u"Status of statement {} is {}.".format(statement_id, status))

            if status not in FINAL_STATEMENT_STATUS:
                session.sleep(retries)
                retries += 1
            else:                
                statement_output = statement[u"output"]

                if statement_output is None:
                    return (True, u"")

                if statement_output[u"status"] == u"ok":
                    return (True, statement_output[u"data"][u"text/plain"])
                elif statement_output[u"status"] == u"error":
                    return (False,
                           statement_output[u"evalue"] + u"\n" + u"".join(statement_output[u"traceback"]))
                else:
                    raise LivyUnexpectedStatusException(u"Unknown output status from Livy: '{}'"
                                                        .format(statement_output[u"status"]))
