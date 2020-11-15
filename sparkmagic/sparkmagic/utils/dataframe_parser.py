import re

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

def match_iter(cell):
    start = 0
    _cell = cell         
    while _cell:
        match = dataframe_anywhere.search(_cell, start)
        if not match: return 
        yield match.span() 
        start = match.end()

cell = """+---+------+
          | id|animal|
          +---+------+
          |  1|   bat|
          |  2| mouse|
          |  3| horse|
          +---+------+
"""
# from sparkmagic.utils import dataframe_parser;print(dataframe_parser.dataframe_anywhere.pattern); d = dataframe_parser.dataframe_anywhere; match_iter= dataframe_parser.match_iter ;c = dataframe_parser.cell;c2 = dataframe_parser.cell2; c3 = dataframe_parser.cell3

cell2 = """
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

class DataframeComponents:
    
    def __init__(self, cell):
        first_df = dataframe_anywhere.search(cell).end()

        expected_width = len(header_top)
        header_top = parts['header_top'].strip() 
         # = ['header_top', 'header_content', 'header_bottom', 'footer']
   

        header_content = parts['header_content'].strip()
        header_bottom = parts['header_bottom'].strip()
        footer = parts['footer'].strip()

        
        all([header_top, header_content, header_bottom, footer], lambda x: len(x) == expected_width, )


    def ensure_components_same_width(self):
        pass

    def header(self):
        pass

    def rows(self):
        pass

class Header:
    pass 
class Body:
    pass

def parse_cell(cell):
    pass
