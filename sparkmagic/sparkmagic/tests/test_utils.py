from IPython.core.error import UsageError
from mock import MagicMock
import numpy as np
from nose.tools import assert_equals, assert_is
import pandas as pd
from pandas.testing import assert_frame_equal

from sparkmagic.livyclientlib.exceptions import BadUserDataException
from sparkmagic.utils.utils import parse_argstring_or_throw, records_to_dataframe
from sparkmagic.utils.constants import SESSION_KIND_PYSPARK
from sparkmagic.utils.dataframe_parser import (
    DataframeHtmlParser,
    cell_contains_dataframe,
    CellComponentType,
    cell_components_iter,
    CellOutputHtmlParser,
)

import unittest


def test_parse_argstring_or_throw():
    parse_argstring = MagicMock(side_effect=UsageError("OOGABOOGABOOGA"))
    try:
        parse_argstring_or_throw(
            MagicMock(), MagicMock(), parse_argstring=parse_argstring
        )
        assert False
    except BadUserDataException as e:
        assert_equals(str(e), str(parse_argstring.side_effect))

    parse_argstring = MagicMock(side_effect=ValueError("AN UNKNOWN ERROR HAPPENED"))
    try:
        parse_argstring_or_throw(
            MagicMock(), MagicMock(), parse_argstring=parse_argstring
        )
        assert False
    except ValueError as e:
        assert_is(e, parse_argstring.side_effect)


def test_records_to_dataframe_missing_value_first():
    result = """{"z":100, "y":50}
{"z":25, "nullv":1.0, "y":10}"""

    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame(
        [{"z": 100, "nullv": None, "y": 50}, {"z": 25, "nullv": 1, "y": 10}],
        columns=["z", "nullv", "y"],
    )
    assert_frame_equal(expected, df)


def test_records_to_dataframe_coercing():
    result = """{"z":"100", "y":"2016-01-01"}
{"z":"25", "y":"2016-01-01"}"""

    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame(
        [
            {"z": 100, "y": np.datetime64("2016-01-01")},
            {"z": 25, "y": np.datetime64("2016-01-01")},
        ],
        columns=["z", "y"],
    )
    assert_frame_equal(expected, df)


def test_records_to_dataframe_no_coercing():
    result = """{"z":"100", "y":"2016-01-01"}
{"z":"25", "y":"2016-01-01"}"""

    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, False)
    expected = pd.DataFrame(
        [{"z": "100", "y": "2016-01-01"}, {"z": "25", "y": "2016-01-01"}],
        columns=["z", "y"],
    )
    assert_frame_equal(expected, df)


def test_records_to_dataframe_missing_value_later():
    result = """{"z":25, "nullv":1.0, "y":10}
{"z":100, "y":50}"""

    df = records_to_dataframe(result, SESSION_KIND_PYSPARK, True)
    expected = pd.DataFrame(
        [{"z": 25, "nullv": 1, "y": 10}, {"z": 100, "nullv": None, "y": 50}],
        columns=["z", "nullv", "y"],
    )
    assert_frame_equal(expected, df)


class TestDataframeParsing(unittest.TestCase):
    def test_dataframe_component(self):
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
        dc = DataframeHtmlParser(cell)
        rows = dc.row_iter()
        self.assertDictEqual(next(rows), {"id": "1", "animal": "bat"})
        self.assertDictEqual(next(rows), {"id": "2", "animal": "mouse"})
        self.assertDictEqual(next(rows), {"id": "3", "animal": "horse"})
        with self.assertRaises(StopIteration):
            next(rows)

        cell = """
                    +---+------+
                    | id|animal|
                    +---+------+
                    +---+------+
                """
        dc = DataframeHtmlParser(cell)
        rows = dc.row_iter()
        with self.assertRaises(StopIteration):
            next(rows)

        cell = """
                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse |
                |  3| horse|
                +---+------+

            Only showing the last 20 rows 
        """
        dc = DataframeHtmlParser(cell)
        rows = dc.row_iter()
        self.assertDictEqual(next(rows), {"id": "1", "animal": "bat"})
        with self.assertRaises(ValueError):
            next(rows)

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
        self.assertTrue(
            cell_contains_dataframe(cell), "Matches with leading whitespaces"
        )

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
        self.assertTrue(
            cell_contains_dataframe(cell), "Cell contains multiple dataframes"
        )

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

    def test_cell_components_iter(self):
        cell = """+---+------+
| id|animal|
+---+------+
|  1|   bat|
|  2| mouse|
|  3| horse|
+---+------+

            Only showing the last 20 rows 
        """
        df_spans = cell_components_iter(cell)

        self.assertEqual(next(df_spans), (CellComponentType.DF, 0, 90))
        self.assertEqual(next(df_spans), (CellComponentType.TEXT, 90, 143))
        self.assertEqual(cell[90:143].strip(), "Only showing the last 20 rows")
        with self.assertRaises(StopIteration):
            next(df_spans)

        cell = """

                Random stuff at the start

                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+

                Random stuff in the middle

                +---+------+
                | id|animal|
                +---+------+
                |  1|   cat|
                |  2| couse|
                |  3| corse|
                +---+------+

                Random stuff at the end
                """
        df_spans = cell_components_iter(cell)

        self.assertEqual(next(df_spans), (CellComponentType.TEXT, 0, 45))
        self.assertEqual(cell[0:45].strip(), "Random stuff at the start")
        self.assertEqual(next(df_spans), (CellComponentType.DF, 45, 247))

        self.assertEqual(next(df_spans), (CellComponentType.TEXT, 247, 293))
        self.assertEqual(cell[247:293].strip(), "Random stuff in the middle")

        self.assertEqual(next(df_spans), (CellComponentType.DF, 293, 495))

        self.assertEqual(next(df_spans), (CellComponentType.TEXT, 495, 553))
        self.assertEqual(cell[495:553].strip(), "Random stuff at the end")

        with self.assertRaises(StopIteration):
            next(df_spans)

        cell = """

                Random stuff at the start

                """
        df_spans = cell_components_iter(cell)

        self.assertEqual(next(df_spans), (CellComponentType.TEXT, 0, len(cell)))
        with self.assertRaises(StopIteration):
            next(df_spans)

        cell = ""
        df_spans = cell_components_iter(cell)
        with self.assertRaises(StopIteration):
            next(df_spans)

    def test_output_html_parser(self):
        cell = ""
        self.assertEqual(CellOutputHtmlParser.to_html(cell), "")

        cell = "Some random text"
        self.assertEqual(
            CellOutputHtmlParser.to_html(cell), "<pre>Some random text</pre>"
        )

        cell = """

                Random stuff at the start

                +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
                +---+------+

                Random stuff in the middle
                +---+------+
                | id|animal|
                +---+------+
                |  1|   cat|
                |  2| couse|
                |  3| corse|
                +---+------+

                Random stuff at the end
                """
        self.maxDiff = 1200
        self.assertEqual(
            CellOutputHtmlParser.to_html(cell),
            (
                "<pre>Random stuff at the start</pre><br />"
                "<table><tr><th>id</th><th>animal</th></tr>"
                "<tr><td>1</td><td>bat</td></tr>"
                "<tr><td>2</td><td>mouse</td></tr>"
                "<tr><td>3</td><td>horse</td></tr>"
                "</table><br /><pre>Random stuff in the middle</pre><br />"
                "<table><tr><th>id</th><th>animal</th></tr>"
                "<tr><td>1</td><td>cat</td></tr>"
                "<tr><td>2</td><td>couse</td></tr>"
                "<tr><td>3</td><td>corse</td></tr>"
                "</table><br /><pre>Random stuff at the end</pre>"
            ),
        )


if __name__ == "__main__":
    unittest.main()
