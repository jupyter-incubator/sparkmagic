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

        First line of code needs to have all possible arguments.

        We could have a choice between option above or having to specify value for -f, like:
        i.e.
            valid:      %delete -f True 9
                        %delete 9 -f True
            invalid:    %delete -f 9
                        %delete 9 -f
        We could have a choice between option above or rolling our own parser to have things like:
            valid:      %delete -f 9
                        %delete 9 -f
        I went with the first option since having our own parser is more error prone. We might revisit this decision in
        the future.

        Anything in the first line that ends up in ns.command below will be merged with the subsequent lines.
        """
        parser = argparse.ArgumentParser(prog="SparkCommandParser")
        parser.add_argument("-o", "--output", type=str, default=None)
        parser.add_argument("-f", "--force", action="store_true")
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
            first_line = "%{} {}".format(UserCommandParser.run_command, first_line)

        # Remove percentage sign
        first_line = first_line[1:]

        # Process subcommand ourselves first
        first_line_split = first_line.split(" ", 1)
        if len(first_line_split) == 1:
            subcommand = first_line_split[0]
            rest_of_line = ""
        else:
            subcommand = first_line_split[0]
            rest_of_line = first_line_split[1]

        subcommand = subcommand.lower()

        try:
            ns = self.parser.parse_args(rest_of_line.split())
        except SystemExit:
            usage = self.parser.format_usage()
            raise SyntaxError("Could not parse cell code to send to Spark.\n{}"
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

            return subcommand, ns.force, ns.output, command
