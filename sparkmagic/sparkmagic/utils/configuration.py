# Distributed under the terms of the Modified BSD License.
import copy
import sys
import base64
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from .constants import HOME_PATH, CONFIG_FILE, MAGICS_LOGGER_NAME, LIVY_KIND_PARAM
from .utils import get_livy_kind
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


d = {}
path = join_paths(HOME_PATH, CONFIG_FILE)

    
def override(config, value):
    _override(d, path, config, value)


def override_all(obj):
    _override_all(d, obj)


_with_override = with_override(d, path)


# Configs

 
def get_session_properties(language):
    properties = copy.deepcopy(session_configs())
    properties[LIVY_KIND_PARAM] = get_livy_kind(language)
    return properties


@_with_override
def session_configs():
    return {}


@_with_override
def kernel_python_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}
    
    
def base64_kernel_python_credentials():
    return _credentials_override(kernel_python_credentials)


# No one's gonna use pyspark and pyspark3 notebook on different endpoints. Reuse the old config.
@_with_override
def kernel_python3_credentials():
    return kernel_python_credentials()


def base64_kernel_python3_credentials():
    return base64_kernel_python_credentials()


@_with_override
def kernel_scala_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}


def base64_kernel_scala_credentials():        
    return _credentials_override(kernel_scala_credentials)

@_with_override
def kernel_r_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}


def base64_kernel_r_credentials():
    return _credentials_override(kernel_r_credentials)


@_with_override
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
            MAGICS_LOGGER_NAME: {
                u"handlers": [u"magicsHandler"],
                u"level": u"DEBUG",
                u"propagate": 0
            }
        }
    }


@_with_override
def events_handler_class():
    return EVENTS_HANDLER_CLASS_NAME


@_with_override
def status_sleep_seconds():
    return 2


@_with_override
def statement_sleep_seconds():
    return 2


@_with_override
def wait_for_idle_timeout_seconds():
    return 15


@_with_override
def livy_session_startup_timeout_seconds():
    return 60


@_with_override
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""


@_with_override
def resource_limit_mitigation_suggestion():
    return ""


@_with_override
def ignore_ssl_errors():
    return False


@_with_override
def use_auto_viz():
    return True


@_with_override
def default_maxrows():
    return 2500


@_with_override
def default_samplemethod():
    return "take"


@_with_override
def default_samplefraction():
    return 0.1


@_with_override
def pyspark_dataframe_encoding():
    return u'utf-8'


@_with_override
def heartbeat_refresh_seconds():
    return 30
    

@_with_override
def heartbeat_retry_seconds():
    return 10


@_with_override
def livy_server_heartbeat_timeout_seconds():
    return 0


@_with_override
def server_extension_default_kernel_name():
    return "pysparkkernel"


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
