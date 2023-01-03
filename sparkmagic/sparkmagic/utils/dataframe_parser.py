import re
from collections import OrderedDict
from enum import Enum
from functools import partial

"""
Creates a HTML table from Spark's Dataframe default string representation

Turns this:
    +---+------+
    | id|animal|
    +---+------+
    |  1|   cat|
    |  2| mouse|
    |  3| horse|
    +---+------+

Into:
    id	animal
    1	cat
    2	mouse
    3	horse
    
    <table>
        <tbody>
            <tr><th>id</th><th>animal</th></tr>
            <tr><td>1</td><td>cat</td>
            </tr><tr><td>2</td><td>mouse</td><
            /tr><tr><td>3</td><td>horse</td></tr>
        </tbody>
    </table>
"""


header_top_pattern = r"[ \t\f\v]*(?P<header_top>\+[-+]*\+)[\n\r]?"
header_content_pattern = r"[ \t\f\v]*(?P<header_content>\|.*\|)[\n\r]"
header_bottom_pattern = r"(?P<header_bottom>[ \t\f\v]*\+[-+]*\+[\n\r])"
row_content_pattern = r"([ \t\f\v]*\|.*\|[\n\r])*"
footer_pattern = r"(?P<footer>[ \t\f\v]*\+[-+]*\+$)"

dataframe_pattern_r = re.compile(
    header_top_pattern
    + header_content_pattern
    + header_bottom_pattern
    + row_content_pattern
    + footer_pattern,
    re.MULTILINE,
)


def extractors(header_top, header_content):
    """Creates functions to pull column values out of Spark DF rows.

    Based on the top of a Dataframe header, identifies start and end index for
    each column value.

    012345678901
    +---+------+
    | id|animal|
    +---+------+
    |  1|   cat|
    |  2| mouse|
    |  3| horse|
    +---+------+

    For example, the `id` column is row[0:4] and `animal` is row[4:11]

    :param header_top The header border top comprise of `+` and `-` marking off
                       demarcating different columns.
                       eg `+---+------+`
    :param header_content The string following the header_top, containing the
                            actual column names
                            eg `| id|animal|`
    :return A dict of column: function that can be applied to string-row
            representation of a a Dataframe, eg `|  1|   cat|`

            In our example:
            {'id': lambda row: row[0:4], 'animal': lambda row: row[4:11]}
    """
    header_pluses = re.finditer(r"\+", header_top)

    def _extract(l, r, row, offset=0):
        return row[offset + l : offset + r].strip()

    def _extractor_iter():
        start = next(header_pluses)
        for end in header_pluses:
            yield partial(_extract, start.end(), end.start())
            start = end

    return OrderedDict((x(header_content), x) for x in _extractor_iter())


class CellComponentType(str, Enum):
    TEXT = "text"
    DF = "DF"


def cell_components_iter(cell):
    """Provides spans for each dataframe in a cell.

    1. Determines if the evaluated output of a cell contains a Spark DF
    2. Splits the cell output on Dataframes
    3. Alternates yielding Plain Text and DF spans of evaluated cell output

    For example if our cell looked like this with provided line numbers:

    0
    1           Random stuff at the start

    45          +---+------+
                | id|animal|
                +---+------+
                |  1|   bat|
                |  2| mouse|
                |  3| horse|
    247         +---+------+

                Random stuff in the middle
    293
    The cell components are
    (CellComponent.TEXT, 0, 45)
    (CellComponentType.DF, 45, 247)
    (CellComponentType.TEXT, 247, 293)
    """
    if not cell:
        return
    df_spans = dataframe_pattern_r.finditer(cell)
    if cell_contains_dataframe(cell):
        df_start, df_end = next(df_spans).span()
        if df_start > 0:
            # Some text before the first Dataframe
            yield (CellComponentType.TEXT, 0, df_start)
        while df_start < len(cell):
            yield (CellComponentType.DF, df_start, df_end)
            try:
                start, end = next(df_spans).span()
                if start > df_end:
                    # Some text before the next Dataframe
                    yield (CellComponentType.TEXT, df_end, start)
                df_start, df_end = start, end
            except StopIteration:
                yield (CellComponentType.TEXT, df_end, len(cell))
                return
    else:
        # Cell does not contain a DF. The whole cell is text.
        yield (CellComponentType.TEXT, 0, len(cell))


def cell_contains_dataframe(cell):
    return dataframe_pattern_r.search(cell) is not None


class CellOutputHtmlParser:
    """Parses cell output and converts it to HTML if applicable."""

    @staticmethod
    def to_html(output):
        return "<br />".join(
            [
                CellOutputHtmlParser.to_html_component(c, output)
                for c in cell_components_iter(output)
            ]
        )

    @staticmethod
    def to_html_component(component_span, cell):
        component_type, start, end = component_span
        if component_type == CellComponentType.DF:
            return DataframeHtmlParser(cell, start, end).to_table()
        elif component_type == CellComponentType.TEXT:
            return "<pre>%s</pre>" % cell[start:end].strip()


class DataframeHtmlParser:
    """Parses a Spark Dataframe and presents it as a HTML table."""

    header_top_r = re.compile(header_top_pattern)
    header_content_r = re.compile(header_content_pattern)

    def __init__(self, cell, start=0, end=None):
        """Creates a Dataframe parser for a single dataframe.

        :param cell The evaluated output of a cell.
                    Cell can contain more than one dataframe, but a single
                    DataframeHtmlParser can only parse table headers/rows for a
                    a single dataframe in the substring cell[start:end]
        """
        self.cell_contents = cell
        end = end or len(self.cell_contents)
        header_spans = DataframeHtmlParser.header_top_r.finditer(
            self.cell_contents, start, end
        )
        parts = {
            "header_top": next(header_spans).span(),
            "header_content": DataframeHtmlParser.header_content_r.search(
                self.cell_contents, start, end
            ).span(),
            "header_bottom": next(header_spans).span(),
            "footer": next(header_spans).span(),
        }
        self.header_content_span = parts["header_content"]
        header_content = self._cell_span(self.header_content_span)

        self.expected_width = len(header_content.strip())

        header_top = self._cell_span(parts["header_top"])
        self.extractors = extractors(header_top.strip(), header_content.strip())
        # The content is between the header-bottom and the footer
        self.content_span = (parts["header_bottom"][1], parts["footer"][0])

    def _cell_span(self, span):
        s, e = span
        return self.cell_contents[s:e]

    def _rowspan_iter(self):
        """Extract each row from the contents of a Dataframe."""
        row_delimiters = re.compile(r"\n").finditer(
            self.cell_contents, self.content_span[0], self.content_span[1]
        )
        start = self.content_span[0]
        for row_delimiter in row_delimiters:
            end, next_start = row_delimiter.span()[0], row_delimiter.span()[1]
            yield (start, end)
            start = next_start

    def row_iter(self, transform=None):
        """Extract and transform each row from a Dataframe.

        Defaults to converting a row to a dict {colName: value}
        """
        _transform = transform or (
            lambda r: {col: x(r) for col, x in self.extractors.items()}
        )
        for rowspan in self._rowspan_iter():
            row = self._cell_span(rowspan).strip()
            if len(row) != self.expected_width:
                raise ValueError(
                    """Expected DF rows to be uniform width (%d)
                                 but found %s (%d)"""
                    % (self.expected_width, row, len(row))
                )
            yield _transform(row)

    def to_table(self):
        """Converts the contents of a notebook cell to a HTML table."""

        header_content = self._cell_span(self.header_content_span)
        table_header_html = self._to_tr(header_content.strip(), is_header=True)

        table_row_iter = self.row_iter(transform=self._to_tr)
        table_body = "".join([r for r in table_row_iter])
        return "<table>%s%s</table>" % (table_header_html, table_body)

    def _to_tr(self, row, is_header=False):
        """Converts a spark dataframe row to a HTML row."""
        tag = "th" if is_header else "td"
        row_content = [x(row) for x in self.extractors.values()]
        row_html = "".join(
            [
                '<%s><pre style="word-break: unset; background-color: unset;">%s</pre></%s>'
                % (tag, rc, tag)
                for rc in row_content
            ]
        )
        return "<tr>%s</tr>" % row_html
