from nose.tools import assert_equals

from remotespark.kernels.wrapperkernel.usercodeparser import UserCodeParser
from remotespark.kernels.kernelmagics import KernelMagics


def test_empty_string():
    parser = UserCodeParser()
    cell = ""

    assert_equals("%%spark\n\n ", parser.get_code_to_run(cell))


def test_spark_code():
    parser = UserCodeParser()
    cell = "my code\nand more"

    assert_equals("%%spark\nmy code\nand more\n ", parser.get_code_to_run(cell))


def test_local_single():
    parser = UserCodeParser()
    cell = """%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi\n ", parser.get_code_to_run(cell))


def test_local_double():
    parser = UserCodeParser()
    cell = """%%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi\n ", parser.get_code_to_run(cell))


def test_our_line_magics():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = "%{}".format(magic_name)

    assert_equals("%%{}\n ".format(magic_name), parser.get_code_to_run(cell))


def test_our_line_magics_with_content():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = """%{}
my content
more content""".format(magic_name)

    assert_equals("%%{}\nmy content\nmore content\n ".format(magic_name), parser.get_code_to_run(cell))


def test_other_cell_magic():
    parser = UserCodeParser()
    cell = """%%magic
hi
hi
hi"""

    assert_equals("{}\n ".format(cell), parser.get_code_to_run(cell))


def test_other_line_magic():
    parser = UserCodeParser()
    cell = """%magic
hi
hi
hi"""

    assert_equals(cell, parser.get_code_to_run(cell))
