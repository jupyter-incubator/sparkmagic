from datetime import datetime
import importlib
import remotespark.utils.configuration as conf
import remotespark.utils.constants as constants
from remotespark.utils import utils


class SparkEvents:
    def __init__(self):
        """
        Create an instance from the handler mentioned in the config file.
        """
        module, class_name = conf.events_handler_class().rsplit('.', 1)
        events_handler_module = importlib.import_module(module)
        events_handler = getattr(events_handler_module, class_name)

        self.handler = events_handler()

    @staticmethod
    def get_utc_date_time():
        return datetime.utcnow()

    def emit_session_creation_start_event(self, session_guid, language):
        self._verify_language_ok(language)

        event_name = constants.SESSION_CREATION_START_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language)]

        self._send_to_handler(kwargs_list)

    def emit_session_creation_end_event(self, session_guid, language, session_id, status):
        self._verify_language_ok(language)
        assert session_id >= 0

        event_name = constants.SESSION_CREATION_END_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATUS, status)]

        self._send_to_handler(kwargs_list)

    def emit_statement_execution_start_event(self, session_guid, language, session_id, statement_guid):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_START_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATEMENT_GUID, statement_guid)]

        self._send_to_handler(kwargs_list)

    def emit_statement_execution_end_event(self, session_guid, language, session_id, statement_guid, statement_id,
                                           success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_END_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATEMENT_GUID, statement_guid),
                       (constants.STATEMENT_ID, statement_id),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self._send_to_handler(kwargs_list)

    def emit_sql_execution_start_event(self, session_guid, language, session_id, sql_guid):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_START_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.SQL_GUID, sql_guid)]

        self._send_to_handler(kwargs_list)

    def emit_sql_execution_end_event(self, session_guid, language, session_id, sql_guid, statement_guid,
                                     success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_END_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.SQL_GUID, sql_guid),
                       (constants.STATEMENT_GUID, statement_guid),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self._send_to_handler(kwargs_list)


    def emit_graph_render_event(self, graph_type):
        event_name = constants.GRAPH_RENDER_EVENT
        time_stamp = SparkEvents.get_utc_date_time()

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.GRAPH_TYPE, graph_type)]

        self._send_to_handler(kwargs_list)

    def emit_magic_execution_start_event(self, magic_name, language, magic_guid):
        self._verify_language_ok(language)
        time_stamp = SparkEvents.get_utc_date_time()

        event_name = constants.MAGIC_EXECUTION_START_EVENT

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.MAGIC_NAME, magic_name),
                       (constants.LIVY_KIND, language),
                       (constants.MAGIC_GUID, magic_guid)]

        self._send_to_handler(kwargs_list)

    def emit_magic_execution_end_event(self, magic_name, language, magic_guid,
                                       success, exception_type, exception_message):
        self._verify_language_ok(language)
        time_stamp = SparkEvents.get_utc_date_time()

        event_name = constants.MAGIC_EXECUTION_START_EVENT

        kwargs_list = [(constants.EVENT_NAME, event_name),
                       (constants.TIMESTAMP, time_stamp),
                       (constants.MAGIC_NAME, magic_name),
                       (constants.LIVY_KIND, language),
                       (constants.MAGIC_GUID, magic_guid),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self._send_to_handler(kwargs_list)

    @staticmethod
    def _verify_language_ok(language):
        assert language in constants.SESSION_KINDS_SUPPORTED

    def _send_to_handler(self, kwargs_list):
        kwargs_list = [(constants.INSTANCE_ID, utils.get_instance_id())] + kwargs_list

        assert len(kwargs_list) <= 12

        self.handler.handle_event(kwargs_list)
