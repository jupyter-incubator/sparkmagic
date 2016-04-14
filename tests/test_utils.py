from IPython.core.error import UsageError
from mock import MagicMock
from nose.tools import assert_equals, assert_is

from remotespark.livyclientlib.exceptions import BadUserDataException
from remotespark.utils.utils import parse_argstring_or_throw



def test_parse_argstring_or_throw():
    parse_argstring = MagicMock(side_effect=UsageError('OOGABOOGABOOGA'))
    try:
        parse_argstring_or_throw(MagicMock(), MagicMock(), parse_argstring=parse_argstring)
        assert False
    except BadUserDataException as e:
        assert_equals(str(e), str(parse_argstring.side_effect))

    parse_argstring = MagicMock(side_effect=ValueError('AN UNKNOWN ERROR HAPPENED'))
    try:
        parse_argstring_or_throw(MagicMock(), MagicMock(), parse_argstring=parse_argstring)
        assert False
    except ValueError as e:
        assert_is(e, parse_argstring.side_effect)
