# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import argparse


class UserCommandParser(object):
    run_command = "run"
    config_command = "config"
    sql_command = "sql"
    hive_command = "hive"
    info_command = "info"
    delete_command = "delete"
    clean_up_command = "cleanup"
    logs_command = "logs"
    local_command = "local"

    def __init__(self):
        """Code can have a magic or no magic specified (specified with %word sign). If no magic is specified, %run will
        be added.

        First line of code needs to have all possible arguments. All arguments need to be at the end of the line.
        i.e.
            valid:      %delete 9 -f
            invalid:    %delete -f 9
        We could have a choice between option above or having to specify value for -f, like:
            valid:      %delete -f True 9
            invalid:    %delete -f 9
        I went with the first option above since it's less verbose. For second option:
            parser.add_argument("-f", "--force", default=False, type=bool)

        Anything in the first line that ends up in ns.command below will be merged with the subsequent lines.
        """
        parser = argparse.ArgumentParser(prog="SparkCommandParser")
        parser.add_argument("subcommand", type=str, nargs=1)
        parser.add_argument("-o", "--output", type=str, default=None)
        parser.add_argument("-f", "--force", default=False, nargs="?", const=True, type=bool)
        parser.add_argument("command", type=str, default=[], nargs="*")

        self.parser = parser

    def parse_user_command(self, code):
        # Get lines
        lines = code.split("\n")
        first_line = lines[0]
        other_lines = "\n".join(lines[1:])

        # Normalize 2 signs to 1
        if first_line.startswith("%%"):
            first_line = first_line[1:]

        # When no magic, add run command
        if not first_line.startswith("%"):
            first_line = "%{} {}".format(UserCommandParser.run_command, code)

        # Remove percentage sign
        first_line = first_line[1:]

        try:
            ns = self.parser.parse_args(first_line.split())
        except SystemExit:
            usage = self.parser.format_usage()
            raise SyntaxError("Could not parse cell code to send to Spark. Please specify all flags at the end.\n{}"
                              .format(usage))
        else:
            command = other_lines

            if len(ns.command) > 0:
                first_line_command = " ".join(ns.command)

                if command != "":
                    command = "{}\n{}".format(first_line_command, command)
                else:
                    command = first_line_command
            else:
                command = "\n".join(lines[1:])

            return ns.subcommand[0].lower(), ns.force, ns.output, command
