# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from IPython import get_ipython

class MessagePrinter:
    """
    This class will be the main class for send message to the user through different ways.
    """

    def print_message(self, msg):
        """
        This method is responsible for printing immediate message to the user.
        """
        get_ipython().write(msg + '\n')
