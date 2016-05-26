# Distributed under the terms of the Modified BSD License.
import copy
from hdijupyterutils.configuration import Configuration
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME

from .constants import HOME_PATH, CONFIG_FILE
from .utils import get_livy_kind
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


class SparkMagicConfigurationBase(Configuration):
    """Utility to read configs for sparkmagic.
    """
    def __init__(self):
        super(SparkMagicConfigurationBase, self).__init__(HOME_PATH, CONFIG_FILE)
        
    @staticmethod
    def _credentials_override(f):
        """A decorator that provide special handling for credentials. It still calls _override().
        If 'base64_password' in config is set, it will base64 decode it and returned in return value's 'password' field.
        If 'base64_password' is not set, it will fallback to to 'password' in config.
        """
        def ret(self):
            credentials = Configuration._override(f)()
            base64_decoded_credentials = {k: credentials.get(k) for k in ('username', 'password', 'url')}
            base64_password = credentials.get('base64_password')
            if base64_password is not None:
                try:
                    base64_decoded_credentials['password'] = base64.b64decode(base64_password).decode()
                except Exception:
                    exception_type, exception, traceback = sys.exc_info()
                    msg = "base64_password for %s contains invalid base64 string: %s %s" % (f.__name__, exception_type, exception)
                    raise BadUserConfigurationException(msg)
            return base64_decoded_credentials

        # Hack! We do this so that we can query the .__name__ of the function
        # later to get the name of the configuration dynamically, e.g. for unit tests
        ret.__name__ = f.__name__
        return ret


class SparkMagicConfiguration(SparkMagicConfigurationBase):
    """Utility to read configs for sparkmagic.
    """
    def __init__(self):
        super(SparkMagicConfiguration, self).__init__()
        
    def get_session_properties(self, language):
        properties = copy.deepcopy(self.session_configs())
        properties[u"kind"] = get_livy_kind(language)
        return properties

    @Configuration._override
    def session_configs():
        return {}

    @SparkMagicConfigurationBase._credentials_override
    def kernel_python_credentials():
        return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}

    @SparkMagicConfigurationBase._credentials_override
    def kernel_scala_credentials():
        return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}

    @Configuration._override
    def logging_config():
        return {
            u"version": 1,
            u"formatters": {
                u"magicsFormatter": {
                    u"format": u"%(asctime)s\t%(levelname)s\t%(message)s",
                    u"datefmt": u""
                }
            },
            u"handlers": {
                u"magicsHandler": {
                    u"class": LOGGING_CONFIG_CLASS_NAME,
                    u"formatter": u"magicsFormatter",
                    u"home_path": HOME_PATH
                }
            },
            u"loggers": {
                u"magicsLogger": {
                    u"handlers": [u"magicsHandler"],
                    u"level": u"DEBUG",
                    u"propagate": 0
                }
            }
        }

    @Configuration._override
    def events_handler_class():
        return u"hdijupyterutils.eventshandler.EventsHandler"

    @Configuration._override
    def status_sleep_seconds():
        return 2

    @Configuration._override
    def statement_sleep_seconds():
        return 2

    @Configuration._override
    def wait_for_idle_timeout_seconds():
        return 15

    @Configuration._override
    def livy_session_startup_timeout_seconds():
        return 60

    @Configuration._override
    def fatal_error_suggestion():
        return u"""The code failed because of a fatal error:
    \t{}.

    Some things to try:
    a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
    b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
    c) Restart the kernel."""

    @Configuration._override
    def ignore_ssl_errors():
        return False

    @Configuration._override
    def use_auto_viz():
        return True

    @Configuration._override
    def default_maxrows():
        return 2500

    @Configuration._override
    def default_samplemethod():
        return "take"

    @Configuration._override
    def default_samplefraction():
        return 0.1

    @Configuration._override
    def pyspark_sql_encoding():
        return u'utf-8'
