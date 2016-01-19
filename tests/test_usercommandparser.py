from nose.tools import with_setup, assert_equals

from remotespark.wrapperkernel.usercommandparser import UserCommandParser

parser = None


def _setup():
    global parser

    parser = UserCommandParser()


def _teardown():
    pass


@with_setup(_setup, _teardown)
def test_magics_no_command():
    subcommand, force, output_var, command = parser.parse_user_command('my code')
    assert_equals(UserCommandParser.run_command, subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("my code", command)


@with_setup(_setup, _teardown)
def test_magics_parser_capital_letter():
    subcommand, force, output_var, command = parser.parse_user_command('%Config')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("", command)


@with_setup(_setup, _teardown)
def test_magics_parser_single_percentage():
    subcommand, force, output_var, command = parser.parse_user_command('%config')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%config hi')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("hi", command)

    subcommand, force, output_var, command = parser.parse_user_command('%config -f')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%config -o my_var')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%config of course -o my_var')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("of course", command)

    subcommand, force, output_var, command = parser.parse_user_command('%config hi hello my name -f')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals('hi hello my name', command)

    subcommand, force, output_var, command = parser.parse_user_command('%config {"extra": 2} hi -f True -o my_var')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert_equals("my_var", output_var)
    assert_equals('{"extra": 2} hi', command)


@with_setup(_setup, _teardown)
def test_magics_parser_double_percentage():
    subcommand, force, output_var, command = parser.parse_user_command('%%config')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config hi')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("hi", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config -f')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config -o my_var')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config of course -o my_var')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("of course", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config hi hello my name -f')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals('hi hello my name', command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config {"extra": 2} hi -f True -o my_var')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert_equals("my_var", output_var)
    assert_equals('{"extra": 2} hi', command)


@with_setup(_setup, _teardown)
def test_magics_parser_multiple_lines():
    subcommand, force, output_var, command = parser.parse_user_command('%%config\nmy code')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("my code", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config hi\nmy code')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("hi\nmy code", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config -f\nmy code')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("my code", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config -o my_var\nmy code')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("my code", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config of course -o my_var\nmy code')
    assert_equals("config", subcommand)
    assert_equals(False, force)
    assert_equals("my_var", output_var)
    assert_equals("of course\nmy code", command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config hi hello my name -f\nmy code')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals('hi hello my name\nmy code', command)

    subcommand, force, output_var, command = parser.parse_user_command('%%config {"extra": 2} hi -f True -o my_var\n'
                                                                       'my code')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert_equals("my_var", output_var)
    assert_equals('{"extra": 2} hi\nmy code', command)
