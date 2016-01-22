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
def test_magics_parser_force():
    subcommand, force, output_var, command = parser.parse_user_command('%delete -f 9')
    assert_equals("delete", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("9", command)

    subcommand, force, output_var, command = parser.parse_user_command('%delete 9')
    assert_equals("delete", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("9", command)

    subcommand, force, output_var, command = parser.parse_user_command('%delete 9 -f')
    assert_equals("delete", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("9", command)

    subcommand, force, output_var, command = parser.parse_user_command('%cleanup -f')
    assert_equals("cleanup", subcommand)
    assert_equals(True, force)
    assert output_var is None
    assert_equals("", command)

    subcommand, force, output_var, command = parser.parse_user_command('%cleanup')
    assert_equals("cleanup", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals("", command)


@with_setup(_setup, _teardown)
def test_magics_parser_multiple_lines_command():
    user_command = """hvacText = sc.textFile("path")
hvacSchema = StructType([StructField("date", StringType(), False),StructField("time", StringType(), False),StructField("targettemp", IntegerType(), False),StructField("actualtemp", IntegerType(), False),StructField("buildingID", StringType(), False)])
hvac = hvacText.map(lambda s: s.split(",")).filter(lambda s: s[0] != "Date").map(lambda s:(str(s[0]), str(s[1]), int(s[2]), int(s[3]), str(s[6]) ))
hvacdf = sqlContext.createDataFrame(hvac,hvacSchema)
hvacdf.registerTempTable("hvac")
data = sqlContext.sql("select buildingID, (targettemp - actualtemp) as temp_diff, date from hvac where date = \"6/1/13\"")
data"""

    subcommand, force, output_var, command = parser.parse_user_command(user_command)
    assert_equals("run", subcommand)
    assert_equals(False, force)
    assert output_var is None
    assert_equals(user_command, command)


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

    subcommand, force, output_var, command = parser.parse_user_command('%config {"extra": 2} hi -f -o my_var')
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

    subcommand, force, output_var, command = parser.parse_user_command('%%config {"extra": 2} hi -f -o my_var')
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

    subcommand, force, output_var, command = parser.parse_user_command('%%config {"extra": 2} hi -f -o my_var\n'
                                                                       'my code')
    assert_equals("config", subcommand)
    assert_equals(True, force)
    assert_equals("my_var", output_var)
    assert_equals('{"extra": 2} hi\nmy code', command)
