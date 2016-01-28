from nose.tools import assert_equals

from remotespark.kernels.wrapperkernel.usercodeparser import UserCodeParser
from remotespark.utils.constants import Constants


language = Constants.lang_scala


def test_without_percentage():
    parser = UserCodeParser()
    first_line = ""
    rest = "hi\nhi"
    cell = "{}{}".format(first_line, rest)

    assert_equals("%%spark\n{}".format(rest), parser.get_code_to_run(cell, language))


def test_local_same_line_single():
    parser = UserCodeParser()
    cell = """%local hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_same_line_double():
    parser = UserCodeParser()
    cell = """%%local hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_new_line_single():
    parser = UserCodeParser()
    cell = """%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_new_line_double():
    parser = UserCodeParser()
    cell = """%%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_cell_magic():
    parser = UserCodeParser()
    first_line = "%%magic\nhi\n"
    rest = "hi\nhi"
    cell = "{}{}".format(first_line, rest)

    assert_equals(cell, parser.get_code_to_run(cell, language))


def test_line_magic():
    parser = UserCodeParser()
    first_line = "%magic\nhi\n"
    rest = "hi\nhi"
    cell = "{}{}".format(first_line, rest)

    assert_equals(cell, parser.get_code_to_run(cell, language))


def test_single_comment():
    parser = UserCodeParser()
    cell = """//hi
//hi
%hi"""

    assert_equals("%hi", parser.get_code_to_run(cell, language))


def test_multi_comment():
    parser = UserCodeParser()
    cell = """/*hi
hi
hi*/
%hi"""

    assert_equals("%hi", parser.get_code_to_run(cell, language))


def test_multi_comment_same_line():
    parser = UserCodeParser()
    cell = """/*hi hi*/
%hi"""

    assert_equals("%hi", parser.get_code_to_run(cell, language))


def test_local_same_line_single_comment():
    parser = UserCodeParser()
    cell = """// hi
%local hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_same_line_double_comment():
    parser = UserCodeParser()
    cell = """// hi
%%local hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_new_line_single_comment():
    parser = UserCodeParser()
    cell = """// hi
%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))


def test_local_new_line_double_comment():
    parser = UserCodeParser()
    cell = """// hi
%%local
hi
hi
hi"""

    assert_equals("hi\nhi\nhi", parser.get_code_to_run(cell, language))
