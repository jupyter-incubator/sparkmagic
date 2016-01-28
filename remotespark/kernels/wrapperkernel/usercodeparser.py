# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.constants import Constants


class UserCodeParser(object):
    @staticmethod
    def get_code_to_run(code, language):
        code = code.strip()

        lines = code.split("\n")

        first_line_index = 0

        if len(lines) == 0:
            first_line = code
        else:
            first_line = lines[0].strip()
            in_multi_line = False
            start_used = None
            for i in range(len(lines)):
                line = lines[i].strip()

                comment, in_multi_line, start_used = UserCodeParser._is_comment(language,
                                                                                line,
                                                                                in_multi_line,
                                                                                start_used)
                if not comment:
                    first_line = line
                    first_line_index = i
                    break

        if not first_line.startswith("%"):
            code_to_run = "%%spark\n{}".format(code)
        elif first_line.startswith("%local"):
            code_to_run = UserCodeParser._code_to_run_helper(first_line, lines[first_line_index+1:], 6)
        elif first_line.startswith("%%local"):
            code_to_run = UserCodeParser._code_to_run_helper(first_line, lines[first_line_index+1:], 7)
        else:
            # First line contains a magic that is not %local
            code_to_run = "\n".join(lines[first_line_index:])

        return code_to_run

    @staticmethod
    def _code_to_run_helper(first_line, lines, l):
        rest = "\n".join(lines)

        if len(first_line) > l:
            separator = "\n"
            first_line_next = first_line[l+1:]
        else:
            separator = ""
            first_line_next = first_line[l:]

        return "{}{}{}".format(first_line_next, separator, rest)

    @staticmethod
    def _is_comment(language, line, in_multi_line, start_used):
        """Returns (bool, bool, str) where:
            first bool indicates if line is a comment
            second bool indicates if next line is part of multi line comment
            str indicates the syntax used for beginning of multi line comment"""
        if in_multi_line:
            # We were already in multi line comment, so check if end of line ends it
            if line.endswith(UserCodeParser._multi_line_end(language, start_used)):
                return True, False, None
            return True, True, start_used
        else:
            # Check if beginning of multi line comment.
            # It's safe to assume that if we see a quote character, we won't be dealing with a case of:
            #   my code = ...\
            #   "something"
            # because the first line would have returned with True and we would not be called again.
            possible_beginnings = UserCodeParser._multi_line_beginning(language)
            for beginning in possible_beginnings:
                if line.startswith(beginning):
                    if line.endswith(UserCodeParser._multi_line_end(language,beginning)):
                        # Line begins and ends with comment - similar to single line comment
                        return True, False, None
                    return True, True, beginning

            # Check if single line comment
            if language == Constants.lang_r:
                return False, False, None

            if line.startswith(UserCodeParser._single_line_beginning(language)):
                return True, False, None

        return False, False, None

    @staticmethod
    def _single_line_beginning(language):
        if language == Constants.lang_python:
            return "#"
        elif language == Constants.lang_scala:
            return "//"
        else:
            raise ValueError("Language '{}' does not support single-line comments.".format(language))

    @staticmethod
    def _multi_line_beginning(language):
        if language == Constants.lang_python:
            return ['"""', "'''"]
        elif language == Constants.lang_scala:
            return ["/*"]
        elif language == Constants.lang_r:
            return ["'", '"']
        else:
            raise ValueError("Language '{}' does not support multi-line comments.".format(language))

    @staticmethod
    def _multi_line_end(language, start_used):
        if language == Constants.lang_python:
            return start_used
        elif language == Constants.lang_scala:
            return "*/"
        elif language == Constants.lang_r:
            return start_used
        else:
            raise ValueError("Language '{}' does not support multi-line comments.".format(language))
