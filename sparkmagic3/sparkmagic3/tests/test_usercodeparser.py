# coding=utf-8
from nose.tools import assert_equals

from sparkmagic.kernels.wrapperkernel.usercodeparser import UserCodeParser
from sparkmagic.kernels.kernelmagics import KernelMagics


def test_empty_string():
    parser = UserCodeParser()

    assert_equals(u"", parser.get_code_to_run(u""))


def test_spark_code():
    parser = UserCodeParser()
    cell = u"my code\nand more"

    assert_equals(u"%%spark\nmy code\nand more", parser.get_code_to_run(cell))


def test_local_single():
    parser = UserCodeParser()
    cell = u"""%local
hi
hi
hi"""

    assert_equals(u"hi\nhi\nhi", parser.get_code_to_run(cell))


def test_local_double():
    parser = UserCodeParser()
    cell = u"""%%local
hi
hi
hi"""

    assert_equals(u"hi\nhi\nhi", parser.get_code_to_run(cell))


def test_our_line_magics():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = u"%{}".format(magic_name)

    assert_equals(u"%%{}\n ".format(magic_name), parser.get_code_to_run(cell))


def test_our_line_magics_with_content():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = u"""%{}
my content
more content""".format(magic_name)

    assert_equals(u"%%{}\nmy content\nmore content\n ".format(magic_name), parser.get_code_to_run(cell))


def test_other_cell_magic():
    parser = UserCodeParser()
    cell = u"""%%magic
hi
hi
hi"""

    assert_equals(u"{}".format(cell), parser.get_code_to_run(cell))


def test_other_line_magic():
    parser = UserCodeParser()
    cell = u"""%magic
hi
hi
hi"""

    assert_equals(cell, parser.get_code_to_run(cell))


def test_scala_code():
    parser = UserCodeParser()
    cell = u"""/* Place the cursor in the cell and press SHIFT + ENTER to run */

val fruits = sc.textFile("wasb:///example/data/fruits.txt")
val yellowThings = sc.textFile("wasb:///example/data/yellowthings.txt")"""

    assert_equals(u"%%spark\n{}".format(cell), parser.get_code_to_run(cell))


def test_unicode():
    parser = UserCodeParser()
    cell = u"print 'è🐙🐙🐙🐙'"

    assert_equals(u"%%spark\n{}".format(cell), parser.get_code_to_run(cell))


def test_unicode_in_magics():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = u"""%{}
my content è🐙
more content""".format(magic_name)

    assert_equals(u"%%{}\nmy content è🐙\nmore content\n ".format(magic_name), parser.get_code_to_run(cell))


def test_unicode_in_double_magics():
    parser = UserCodeParser()
    magic_name = KernelMagics.info.__name__
    cell = u"""%%{}
my content è🐙
more content""".format(magic_name)

    assert_equals(u"%%{}\nmy content è🐙\nmore content\n ".format(magic_name), parser.get_code_to_run(cell))
