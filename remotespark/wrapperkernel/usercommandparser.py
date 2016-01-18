# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


class UserCommandParser(object):
    run_command = "run"
    config_command = "config"
    sql_command = "sql"
    hive_command = "hive"
    info_command = "info"
    delete_command = "delete"
    clean_up_command = "cleanup"
    logs_command = "logs"

    force_flag = "f"

    @staticmethod
    def parse_user_command(code):
        # Normalize 2 signs to 1
        if code.startswith("%%"):
            code = code[1:]

        # When no magic, return run command
        if not code.startswith("%"):
            code = "%{} {}".format(UserCommandParser.run_command, code)

        # Remove percentage sign
        code = code[1:]

        split_code = code.split(None, 1)
        subcommand = split_code[0].lower()
        flags = []
        if len(split_code) > 1:
            rest = split_code[1]
        else:
            rest = ""

        # Get all flags
        flag_split = rest.split(None, 1)
        while len(flag_split) >= 1 and flag_split[0].startswith("-"):
            if len(flag_split) >= 2:
                flags.append(flag_split[0][1:].lower())
                rest = flag_split[1]
                flag_split = rest.split(None, 1)
            if len(flag_split) == 1:
                flags.append(flag_split[0][1:].lower())
                flag_split = [""]

        # flags to lower
        flags = [i.lower() for i in flags]

        return subcommand, flags, rest
