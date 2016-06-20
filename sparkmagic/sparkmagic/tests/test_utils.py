from IPython.core.error import UsageError
from mock import MagicMock
from nose.tools import assert_equals, assert_is

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from sparkmagic.utils.utils import parse_argstring_or_throw
from sparkmagic.utils.utils import get_html_link


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

def test_link():
    url = u"https://microsoft.com"
    assert_equals(get_html_link(u'Link', url), u"""<a target="_blank" href="https://microsoft.com">Link</a>""")

    url = None
    assert_equals(get_html_link(u'Link', url), u"")
