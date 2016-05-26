# Distributed under the terms of the Modified BSD License.
from IPython.core.error import UsageError
from IPython.core.magic_arguments import parse_argstring

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from .constants import LANG_SCALA, LANG_PYTHON, LANG_R, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK


def get_livy_kind(language):
    if language == LANG_SCALA:
        return SESSION_KIND_SPARK
    elif language == LANG_PYTHON:
        return SESSION_KIND_PYSPARK
    elif language == LANG_R:
        return SESSION_KIND_SPARKR
    else:
        raise ValueError("Cannot get session kind for {}.".format(language))
        
        
def parse_argstring_or_throw(magic_func, argstring, parse_argstring=parse_argstring):
    """An alternative to the parse_argstring method from IPython.core.magic_arguments.
    Catches IPython.core.error.UsageError and propagates it as a
    livyclientlib.exceptions.BadUserDataException."""
    try:
        return parse_argstring(magic_func, argstring)
    except UsageError as e:
        raise BadUserDataException(str(e))
