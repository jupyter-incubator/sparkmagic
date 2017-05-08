from IPython.core.error import UsageError
from mock import MagicMock
from nose.tools import assert_equals, assert_is
import pandas as pd
from pandas.util.testing import assert_frame_equal

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from sparkmagic.utils.utils import parse_argstring_or_throw, records_to_dataframe
from sparkmagic.utils.constants import SESSION_KIND_PYSPARK


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


def test_records_to_dataframe_missing_value_first():
    result = """{"z":100, "y":50}
{"z":25, "nullv":1.0, "y":10}"""
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK)
    expected = pd.DataFrame([{'z': 100, "nullv": None, 'y': 50}, {'z':25, "nullv":1, 'y':10}], columns=['z', "nullv", 'y'])
    assert_frame_equal(expected, df)


def test_records_to_dataframe_missing_value_later():
    result = """{"z":25, "nullv":1.0, "y":10}
{"z":100, "y":50}"""
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK)
    expected = pd.DataFrame([{'z':25, "nullv":1, 'y':10}, {'z': 100, "nullv": None, 'y': 50}], columns=['z', "nullv", 'y'])
    assert_frame_equal(expected, df)
