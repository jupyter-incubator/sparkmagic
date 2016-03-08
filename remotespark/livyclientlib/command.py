import textwrap

from remotespark.utils.guid import ObjectWithGuid
from remotespark.utils.log import Log


class Command(ObjectWithGuid):
    def __init__(self, code):
        super(Command, self).__init__()
        self.code = textwrap.dedent(code)
        self.logger = Log("Command")

    def __eq__(self, other):
        return self.code == other.code

    def __ne__(self, other):
        return not self == other

    def execute(self, session):
        session.wait_for_idle()
        data = {"code": self.code}
        r = session.http_client.post_statement(session.id, data)
        statement_id = r['id']
        return self._get_statement_output(session, statement_id)

    def _get_statement_output(self, session, statement_id):
        statement_running = True
        out = ""
        while statement_running:
            r = session.http_client.get_statement(session.id, statement_id)
            statement = r
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
