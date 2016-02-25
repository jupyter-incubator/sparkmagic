# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .livyclient import LivyClient


class RLivyClient(LivyClient):
    """Spark client for Livy session in R"""

    def _get_command_for_query(self, sqlquery):
        raise NotImplementedError()
