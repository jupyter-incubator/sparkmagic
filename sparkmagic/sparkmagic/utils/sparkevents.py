import importlib
from hdijupyterutils.constants import EVENT_NAME, TIMESTAMP
from hdijupyterutils.events import Events

from . import configuration as conf
import sparkmagic.utils.constants as constants


def get_spark_events_handler():
    """
    Create an instance from the handler mentioned in the config file.
    """
    module, class_name = conf.events_handler_class().rsplit('.', 1)
    events_handler_module = importlib.import_module(module)
    events_handler = getattr(events_handler_module, class_name)
    handler = events_handler(constants.MAGICS_LOGGER_NAME, conf.logging_config())
    return handler


class SparkEvents(Events):
    def __init__(self):
        handler = get_spark_events_handler()
        super(SparkEvents, self).__init__(handler)
    

    def emit_library_loaded_event(self):
        event_name = constants.LIBRARY_LOADED_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp)]

        self.send_to_handler(kwargs_list)

    def emit_cluster_change_event(self, cluster_dns_name, status_code, success, error_message):
        event_name = constants.CLUSTER_CHANGE_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.CLUSTER_DNS_NAME, cluster_dns_name),
                       (constants.STATUS_CODE, status_code),
                       (constants.SUCCESS, success),
                       (constants.ERROR_MESSAGE, error_message)]

        self.send_to_handler(kwargs_list)

    def emit_session_creation_start_event(self, session_guid, language):
        self._verify_language_ok(language)

        event_name = constants.SESSION_CREATION_START_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language)]

        self.send_to_handler(kwargs_list)

    def emit_session_creation_end_event(self, session_guid, language, session_id, status,
                                        success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.SESSION_CREATION_END_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATUS, status),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self.send_to_handler(kwargs_list)

    def emit_session_deletion_start_event(self, session_guid, language, session_id, status):
        self._verify_language_ok(language)

        event_name = constants.SESSION_DELETION_START_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATUS, status)]

        self.send_to_handler(kwargs_list)

    def emit_session_deletion_end_event(self, session_guid, language, session_id, status,
                                        success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.SESSION_DELETION_END_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATUS, status),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self.send_to_handler(kwargs_list)

    def emit_statement_execution_start_event(self, session_guid, language, session_id, statement_guid):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_START_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATEMENT_GUID, statement_guid)]

        self.send_to_handler(kwargs_list)

    def emit_statement_execution_end_event(self, session_guid, language, session_id, statement_guid, statement_id,
                                           success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.STATEMENT_EXECUTION_END_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.STATEMENT_GUID, statement_guid),
                       (constants.STATEMENT_ID, statement_id),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self.send_to_handler(kwargs_list)

    def emit_sql_execution_start_event(self, session_guid, language, session_id, sql_guid,
                                       samplemethod, maxrows, samplefraction):
        self._verify_language_ok(language)

        event_name = constants.SQL_EXECUTION_START_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.SQL_GUID, sql_guid),
                       (constants.SAMPLE_METHOD, samplemethod),
                       (constants.MAX_ROWS, maxrows),
                       (constants.SAMPLE_FRACTION, samplefraction)]

        self.send_to_handler(kwargs_list)

    def emit_sql_execution_end_event(self, session_guid, language, session_id, sql_guid, statement_guid,
                                     success, exception_type, exception_message):
        self._verify_language_ok(language)

        event_name = constants.SQL_EXECUTION_END_EVENT
        time_stamp = self.get_utc_date_time()

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.SESSION_GUID, session_guid),
                       (constants.LIVY_KIND, language),
                       (constants.SESSION_ID, session_id),
                       (constants.SQL_GUID, sql_guid),
                       (constants.STATEMENT_GUID, statement_guid),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self.send_to_handler(kwargs_list)

    def emit_magic_execution_start_event(self, magic_name, language, magic_guid):
        self._verify_language_ok(language)
        time_stamp = self.get_utc_date_time()

        event_name = constants.MAGIC_EXECUTION_START_EVENT

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.MAGIC_NAME, magic_name),
                       (constants.LIVY_KIND, language),
                       (constants.MAGIC_GUID, magic_guid)]

        self.send_to_handler(kwargs_list)

    def emit_magic_execution_end_event(self, magic_name, language, magic_guid,
                                       success, exception_type, exception_message):
        self._verify_language_ok(language)
        time_stamp = self.get_utc_date_time()

        event_name = constants.MAGIC_EXECUTION_END_EVENT

        kwargs_list = [(EVENT_NAME, event_name),
                       (TIMESTAMP, time_stamp),
                       (constants.MAGIC_NAME, magic_name),
                       (constants.LIVY_KIND, language),
                       (constants.MAGIC_GUID, magic_guid),
                       (constants.SUCCESS, success),
                       (constants.EXCEPTION_TYPE, exception_type),
                       (constants.EXCEPTION_MESSAGE, exception_message)]

        self.send_to_handler(kwargs_list)

    @staticmethod
    def _verify_language_ok(language):
        assert language in constants.SESSION_KINDS_SUPPORTED

