import re

header_start = r'[ \t\f\v]*\+[-+]*\+[\n\r]'
# I could not find any documentation on restrictions on spark column names 
header_content = r'[ \t\f\v]*(\|.*\|)[\n\r]'
header_bottom = r'[ \t\f\v]*\+[-+]*\+[\n\r]'
row_content = r'([ \t\f\v]*\|.*\|[\n\r])*'
footer = r'[ \t\f\v]*\+[-+]*\+$'
'''
from sparkmagic.utils import dataframe_parser;print(dataframe_parser.dataframe_anywhere.pattern)
'''
dataframe_anywhere = re.compile(f'{header_start}{header_content}{header_bottom}{row_content}{footer}', re.MULTILINE)

def cell_contains_dataframe(cell):
    return dataframe_anywhere.search(cell) is not None

class Header:
    pass 
class Body:
    pass

def parse_cell(cell):
    pass
