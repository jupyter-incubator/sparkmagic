def get_session_properties(language):
    properties = copy.deepcopy(session_configs())
    properties[u"kind"] = get_livy_kind(language)
    return properties


# All of the functions below return the values of configurations. They are
# all marked with the _override decorator, which returns the overridden
# value of that configuration if there is any such configuration. Otherwise,
# these functions return the default values described in their bodies.


@_override
def session_configs():
    return {}


def _credentials_override(f):
    """A decorator that provide special handling for credentials. It still calls _override().
    If 'base64_password' in config is set, it will base64 decode it and returned in return value's 'password' field.
    If 'base64_password' is not set, it will fallback to to 'password' in config.
    """
    def ret():
        credentials = _override(f)()
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


@_credentials_override
def kernel_python_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}


@_credentials_override
def kernel_scala_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998'}


@_override
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
                u"class": u"hdijupyterutils.filehandler.MagicsFileHandler",
                u"formatter": u"magicsFormatter"
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

@_override
def events_handler_class():
    return u"hdijupyterutils.eventshandler.EventsHandler"


@_override
def status_sleep_seconds():
    return 2


@_override
def statement_sleep_seconds():
    return 2


@_override
def wait_for_idle_timeout_seconds():
    return 15


@_override
def livy_session_startup_timeout_seconds():
    return 60


@_override
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""


@_override
def ignore_ssl_errors():
    return False


@_override
def use_auto_viz():
    return True


@_override
def default_maxrows():
    return 2500


@_override
def default_samplemethod():
    return "take"


@_override
def default_samplefraction():
    return 0.1


@_override
def pyspark_sql_encoding():
    return u'utf-8'