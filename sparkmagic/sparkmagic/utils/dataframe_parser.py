import re
import itertools 
from functools import partial

header_top = r'[ \t\f\v]*(?P<header_top>\+[-+]*\+)[\n\r]'
# I could not find any documentation on restrictions on spark column names 
header_content = r'[ \t\f\v]*(?P<header_content>\|.*\|)[\n\r]'
header_bottom = r'(?P<header_bottom>[ \t\f\v]*\+[-+]*\+[\n\r])'
row_content = r'([ \t\f\v]*\|.*\|[\n\r])*'
footer = r'(?P<footer>[ \t\f\v]*\+[-+]*\+$)'
row_content_inner = r'(\|.*\|)'

'''
import re
from sparkmagic.utils import dataframe_parser;print(dataframe_parser.dataframe_anywhere.pattern); d = dataframe_parser.dataframe_anywhere;c = dataframe_parser.cell;d.match(c).groupdict()
hbt, hbb = d.match(c).span('header_bottom')
ft, fb = d.match(c).span('footer')
cc = c[hbb:ft]
rc = dataframe_parser.row_content
re.findall(rc, cc)
'''
dataframe_anywhere = re.compile(f'{header_top}{header_content}{header_bottom}{row_content}{footer}', re.MULTILINE)

def get_extractors(header_top):
    """ Creates functions to pull column values out of Spark DF rows.

    The functions return substrings based on where the '+'s are in header_top.
    """
    header_pluses = re.finditer(r"\+", header_top)
    start = next(header_pluses)

    def _extract(l, r, row, offset=0):
        return row[offset+l:offset+r].strip()

    for end in header_pluses:
        yield partial(_extract, start.end(), end.start())
        start = end

cell = """+---+------+
          | id|animal|
          +---+------+
          |  1|   bat|
          |  2| mouse|
          |  3| horse|
          +---+------+
"""
# from sparkmagic.utils import dataframe_parser;print(dataframe_parser.dataframe_anywhere.pattern); d = dataframe_parser.dataframe_anywhere; ;c = dataframe_parser.cell;c2 = dataframe_parser.cell2; c3 = dataframe_parser.cell3

cell2 = """
        +---+------+
        | id|animal|
        +---+------+
        +---+------+
        """
cell3 = """
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

def cell_contains_dataframe(cell):
    return dataframe_anywhere.search(cell) is not None

# import re;from sparkmagic.utils import dataframe_parser;print(dataframe_parser.dataframe_anywhere.pattern); d = dataframe_parser.dataframe_anywhere;c = dataframe_parser.cell;c2 = dataframe_parser.cell2; c3 = dataframe_parser.cell3; m = list(re.finditer(d,c))[0]; dc = dataframe_parser.DataframeComponents(c)

class DataframeComponents:
    """Instantiates utilities getting rows and columns out of a dataframe."""
    
    def __init__(self, cell):
        """ Creates a Dataframe parser for a single dataframe.
        
        :param cell A single Dataframe, usually the result of 
                    re.finditer(dataframe_anywhere, cells)
        """
        header_spans = re.finditer(header_top, cell)
        self.parts = {
            'header_top': next(header_spans).span(),
            'header_content': re.search(header_content, cell).span(),
            'header_bottom': next(header_spans).span(),
            'footer': next(header_spans).span()
        }

        hc_start, hc_end = self.parts['header_content'] 
        header_columns = cell[hc_start: hc_end].strip()
        self.expected_width = len(header_columns.strip())

        ht_start, ht_end = self.parts['header_top']
        extractors = get_extractors(cell[ht_start: ht_end].strip())
        self.extractors = { x(header_columns):x for x in extractors }
        self.header = self.extractors.keys()
        self.rowspan = (self.parts['header_bottom'][1], 
                        self.parts['footer'][0])
        
    def get_header(self):
        return self.header

    def get_rowspan(self):
        self.rowspan
    
    def empty(self):
        return (self.rowspan[1] - self.rowspan[0]) == 0

    def rowspan_iter(self, cell):
        row_delimiters = re.compile(r"\n").finditer(cell,
                                                    self.rowspan[0], 
                                                    self.rowspan[1])
        start = self.rowspan[0]
        for row_delimiter in row_delimiters:
            end, next_start = row_delimiter.span()[0], row_delimiter.span()[1]
            yield (start, end)
            start = next_start
    
    def row_iter(self, cell):
        rowspans = self.rowspan_iter(cell)
        for rowspan in rowspans:
            s, e = rowspan
            row = cell[s: e].strip()
            if len(row) != self.expected_width:
                raise ValueError(f"Expected DF rows to be uniform width"
                                 f" ({self.expected_width})"
                                 f" but found {row} {len(row)}")
            yield {col: x(row) for col, x in self.extractors.items()}
