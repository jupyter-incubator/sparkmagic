import sys
import textwrap
import base64

from IPython.display import Image

from ipywidgets.widgets import FloatProgress, Layout

from hdijupyterutils.guid import ObjectWithGuid

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.constants import (
    MAGICS_LOGGER_NAME,
    FINAL_STATEMENT_STATUS,
    MIMETYPE_IMAGE_PNG,
    MIMETYPE_TEXT_HTML,
    MIMETYPE_TEXT_PLAIN,
    COMMAND_INTERRUPTED_MSG,
    COMMAND_CANCELLATION_FAILED_MSG,
    MIMETYPE_APPLICATION_JSON,
)
from .exceptions import (
    LivyUnexpectedStatusException,
    SparkStatementCancelledException,
    SparkStatementCancellationFailedException,
)


class Command(ObjectWithGuid):
    def __init__(self, code, spark_events=None):
        super(Command, self).__init__()
        self.code = textwrap.dedent(code)
        self.logger = SparkLog("Command")
        if spark_events is None:
            spark_events = SparkEvents()
        self._spark_events = spark_events

    def __repr__(self):
        return "Command({}, ...)".format(repr(self.code))

    def __eq__(self, other):
        return self.code == other.code

    def __ne__(self, other):
        return not self == other

    def execute(self, session):
        self._spark_events.emit_statement_execution_start_event(
            session.guid, session.kind, session.id, self.guid
        )
        statement_id = -1
        try:
            session.wait_for_idle()
            data = {"code": self.code}
            response = session.http_client.post_statement(session.id, data)
            statement_id = response["id"]
            output = self._get_statement_output(session, statement_id)
        except KeyboardInterrupt as e:
            self._spark_events.emit_statement_execution_end_event(
                session.guid,
                session.kind,
                session.id,
                self.guid,
                statement_id,
                False,
                e.__class__.__name__,
                str(e),
            )
            try:
                if statement_id >= 0:
                    response = session.http_client.cancel_statement(
                        session.id, statement_id
                    )
                    session.wait_for_idle()
            except:
                raise SparkStatementCancellationFailedException(
                    COMMAND_CANCELLATION_FAILED_MSG
                )
            else:
                raise SparkStatementCancelledException(COMMAND_INTERRUPTED_MSG)
        except Exception as e:
            self._spark_events.emit_statement_execution_end_event(
                session.guid,
                session.kind,
                session.id,
                self.guid,
                statement_id,
                False,
                e.__class__.__name__,
                str(e),
            )
            raise
        else:
            self._spark_events.emit_statement_execution_end_event(
                session.guid,
                session.kind,
                session.id,
                self.guid,
                statement_id,
                True,
                "",
                "",
            )
            return output

    def _get_statement_output(self, session, statement_id):
        retries = 1
        progress = FloatProgress(
            value=0.0,
            min=0,
            max=1.0,
            step=0.01,
            description="Progress:",
            bar_style="info",
            orientation="horizontal",
            layout=Layout(width="50%", height="25px"),
        )
        session.ipython_display.display(progress)

        while True:
            statement = session.http_client.get_statement(session.id, statement_id)
            status = statement["state"].lower()

            self.logger.debug(
                "Status of statement {} is {}.".format(statement_id, status)
            )

            if status not in FINAL_STATEMENT_STATUS:
                progress.value = statement.get("progress", 0.0)
                session.sleep(retries)
                retries += 1
            else:
                statement_output = statement["output"]
                progress.close()

                if statement_output is None:
                    return (True, "", MIMETYPE_TEXT_PLAIN)

                if statement_output["status"] == "ok":
                    data = statement_output["data"]
                    if MIMETYPE_IMAGE_PNG in data:
                        image = Image(base64.b64decode(data[MIMETYPE_IMAGE_PNG]))
                        return (True, image, MIMETYPE_IMAGE_PNG)
                    elif MIMETYPE_TEXT_HTML in data:
                        return (True, data[MIMETYPE_TEXT_HTML], MIMETYPE_TEXT_HTML)
                    elif MIMETYPE_APPLICATION_JSON in data:
                        return (
                            True,
                            data[MIMETYPE_APPLICATION_JSON],
                            MIMETYPE_APPLICATION_JSON,
                        )
                    else:
                        return (True, data[MIMETYPE_TEXT_PLAIN], MIMETYPE_TEXT_PLAIN)
                elif statement_output["status"] == "error":
                    return (
                        False,
                        statement_output["evalue"]
                        + "\n"
                        + "".join(statement_output["traceback"]),
                        MIMETYPE_TEXT_PLAIN,
                    )
                else:
                    raise LivyUnexpectedStatusException(
                        "Unknown output status from Livy: '{}'".format(
                            statement_output["status"]
                        )
                    )
