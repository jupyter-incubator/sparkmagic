# Distributed under the terms of the Modified BSD License.
import copy
import sys
import base64
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from .constants import HOME_PATH, CONFIG_FILE
from .utils import get_livy_kind
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


d = {}
path = join_paths(HOME_PATH, CONFIG_FILE)

    
def override(config, value):
    global d, path
    _override(d, path, config, value)


def override_all(obj):
    global d
    _override_all(d, obj)


# Configs
       
def get_session_properties(language):
    properties = copy.deepcopy(session_configs())
    properties[u"kind"] = get_livy_kind(language)
    return properties

@with_override(d, path)
def session_configs():
    return {}

@with_override(d, path)
def kernel_python_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}
    
def base64_kernel_python_credentials():
    return _credentials_override(kernel_python_credentials)       

@with_override(d, path)
def kernel_scala_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}

def base64_kernel_scala_credentials():        
    return _credentials_override(kernel_scala_credentials)

@with_override(d, path)
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

@with_override(d, path)
def events_handler_class():
    return EVENTS_HANDLER_CLASS_NAME

@with_override(d, path)
def status_sleep_seconds():
    return 2

@with_override(d, path)
def statement_sleep_seconds():
    return 2

@with_override(d, path)
def wait_for_idle_timeout_seconds():
    return 15

@with_override(d, path)
def livy_session_startup_timeout_seconds():
    return 60

@with_override(d, path)
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""

@with_override(d, path)
def ignore_ssl_errors():
    return False

@with_override(d, path)
def use_auto_viz():
    return True

@with_override(d, path)
def default_maxrows():
    return 2500

@with_override(d, path)
def default_samplemethod():
    return "take"

@with_override(d, path)
def default_samplefraction():
    return 0.1

@with_override(d, path)
def pyspark_sql_encoding():
    return u'utf-8'
    
def _credentials_override(f):
    """Provides special handling for credentials. It still calls _override().
    If 'base64_password' in config is set, it will base64 decode it and returned in return value's 'password' field.
    If 'base64_password' is not set, it will fallback to to 'password' in config.
    """
    credentials = f()
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
