# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.constants import Constants


class UserCodeTransformerBase(object):
    def __init__(self, subcommand):
        self.name = subcommand

    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        raise NotImplementedError()


class NotSupportedTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = "Magic '{}' not supported.".format(self.name)
        code_to_run = ""
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class ConfigTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        if session_started:
            if not force:
                error_to_show = "A session has already been started. In order to modify the Spark configura" \
                                "tion, please provide the '-f' flag at the beginning of the config magic:\n" \
                                "\te.g. `%config -f {}`\n\nNote that this will kill the current session and" \
                                " will create a new one with the configuration provided. All previously run " \
                                "commands in the session will be lost."
                code_to_run = ""
            else:
                begin_action = Constants.delete_session_action
                end_action = Constants.start_session_action
                code_to_run = "%spark config {}".format(command)
        else:
            code_to_run = "%spark config {}".format(command)

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class SparkTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        code_to_run = "%%spark\n{}".format(command)
        begin_action = Constants.start_session_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class ContextOutputVariableTransformer(UserCodeTransformerBase):
    def __init__(self, subcommand):
        super(ContextOutputVariableTransformer, self).__init__(subcommand)

        self.context_name = None

    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        code_to_run = "%%spark -c {}\n{}".format(self.context_name, command)
        begin_action = Constants.start_session_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class SqlTransformer(ContextOutputVariableTransformer):
    def __init__(self, subcommand):
        super(SqlTransformer, self).__init__(subcommand)

        self.context_name = Constants.context_name_sql


class HiveTransformer(ContextOutputVariableTransformer):
    def __init__(self, subcommand):
        super(HiveTransformer, self).__init__(subcommand)

        self.context_name = Constants.context_name_hive


class InfoTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        code_to_run = "%spark info {}".format(connection_string)
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class DeleteSessionTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        if not force:
            error_to_show = "The session you are trying to delete could be this kernel's session. In order " \
                            "to delete this session, please provide the '-f' flag at the beginning of the " \
                            "delete magic:\n\te.g. `%delete -f id`\n\nAll previously run commands in the " \
                            "session will be lost."
            code_to_run = ""
        else:
            deletes_session = True
            code_to_run = "%spark delete {} {}".format(connection_string, command)

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class CleanUpTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        if not force:
            error_to_show = "The sessions you are trying to delete could be this kernel's session or other " \
                            "people's sessions. In order to delete them, please provide the '-f' flag at the " \
                            "beginning of the cleanup magic:\n\te.g. `%cleanup -f`\n\nAll previously run " \
                            "commands in the sessions will be lost."
            code_to_run = ""
        else:
            deletes_session = True
            code_to_run = "%spark cleanup {}".format(connection_string)

        return code_to_run, error_to_show, begin_action, end_action, deletes_session


class LogsTransformer(UserCodeTransformerBase):
    def get_code_to_execute(self, session_started, connection_string, force, output_var, command):
        error_to_show = None
        begin_action = Constants.do_nothing_action
        end_action = Constants.do_nothing_action
        deletes_session = False

        if session_started:
            code_to_run = "%spark logs"
        else:
            code_to_run = "print('No logs yet.')"

        return code_to_run, error_to_show, begin_action, end_action, deletes_session
