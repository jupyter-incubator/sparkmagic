from IPython.core.error import UsageError
from mock import MagicMock
import numpy as np
from nose.tools import assert_equals, assert_is
import pandas as pd
from pandas.util.testing import assert_frame_equal

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from sparkmagic.utils.utils import parse_argstring_or_throw, records_to_dataframe
from sparkmagic.utils.constants import SESSION_KIND_PYSPARK
from sparkmagic.utils.dataframe_parser import *

import unittest


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
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame([{'z': 100, "nullv": None, 'y': 50}, {'z':25, "nullv":1, 'y':10}], columns=['z', "nullv", 'y'])
    assert_frame_equal(expected, df)


def test_records_to_dataframe_coercing():
    result = """{"z":"100", "y":"2016-01-01"}
{"z":"25", "y":"2016-01-01"}"""
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame([{'z': 100, 'y': np.datetime64("2016-01-01")}, {'z':25, 'y':np.datetime64("2016-01-01")}], columns=['z', 'y'])
    assert_frame_equal(expected, df)


def test_records_to_dataframe_no_coercing():
    result = """{"z":"100", "y":"2016-01-01"}
{"z":"25", "y":"2016-01-01"}"""
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, False)
    expected = pd.DataFrame([{'z': "100", 'y': "2016-01-01"}, {'z':"25", 'y':"2016-01-01"}], columns=['z', 'y'])
    assert_frame_equal(expected, df)


def test_records_to_dataframe_missing_value_later():
    result = """{"z":25, "nullv":1.0, "y":10}
{"z":100, "y":50}"""
    
    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame([{'z':25, "nullv":1, 'y':10}, {'z': 100, "nullv": None, 'y': 50}], columns=['z', "nullv", 'y'])
    assert_frame_equal(expected, df)


class TestDataframeParsing(unittest.TestCase):
    """
    python /Users/gary.he/dev/sparkmagic/sparkmagic/sparkmagic/tests/test_utils.py TestDataframeParsing.test_dataframe_parsing
    from sparkmagic.utils import dataframe_parser, match_iter;print(dataframe_parser.dataframe_anywhere.pattern); d = dataframe_parser.dataframe_anywhere;c = dataframe_parser.cell;d.match(c).groupdict()

    """
    def test__match_iter(self):
        cell = ""
        with self.assertRaises(StopIteration):
            next(match_iter(cell))

        cell = """
          +---+------+
          | id|animal|
          +---+------+
          |  1|   bat|
          |  2| mouse|
          |  3| horse|
          +---+------+
        """
        self.assertCountEqual(list(match_iter(cell)), [(1, 161)])

        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+

                +---+------+
                | id|animal|
                +---+------+
                |  1|   cat|
                |  2| louse|
                |  3| morse|
                +---+------+

                """
        self.assertCountEqual(list(match_iter(cell)), [(1, 203), (205, 407)], "Multiple DFs")

        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+
                        
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+----/-+
                        
                +---+------+
                | id|animal|
                +---+------+
                |  1|   cat|
                |  2| couse|
                |  3| corse|
                +---+------+
        """
        self.assertCountEqual(list(match_iter(cell)), [(1, 203), (457, 659)], "DF in the middle is not valid")


    def test_dataframe_parsing(self):
        
        cell = """+---+------+
| id|animal|
+---+------+
|  1|   bat|
|  2| mouse|
|  3| horse|
+---+------+

            Only showing the last 20 rows 
        """
        self.assertTrue(cell_contains_dataframe(cell))

        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+

            Only showing the last 20 rows 
        """
        self.assertTrue(cell_contains_dataframe(cell), "Matches with leading whitespaces")


        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+

                +---+------+
                | id|animal|
                +---+------+
                |  1|   cat|
                |  2| couse|
                |  3| corse|
                +---+------+

                """
        self.assertTrue(cell_contains_dataframe(cell), "Cell contains multiple dataframes")

        cell = """
                    +---+------+
                    | id|animal|
                    +---+------+
                    +---+------+
                """
        self.assertTrue(cell_contains_dataframe(cell), "Empty DF")

        cell = """
                    +---+
                    | id|
                    +---+
                    +---+
                """
        self.assertTrue(cell_contains_dataframe(cell), "Single Column DF")

        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+----/-+

                Only showing the last 20 rows 
                """
        self.assertFalse(cell_contains_dataframe(cell), "Footer contains a /")

if __name__ == '__main__':
    unittest.main()