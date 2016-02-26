# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from remotespark.utils.constants import LONG_RANDOM_VARIABLE_NAME
from .livyclient import LivyClient


class PysparkLivyClient(LivyClient):
    """Spark client for Livy session in PySpark"""

    def _get_command_for_query(self, sqlquery):
        command = 'sqlContext.sql("""{}""")'.format(sqlquery.query)
        if sqlquery.only_columns:
            command = '{}.columns'.format(command)
        else:
            command = '{}.toJSON()'.format(command)
            if sqlquery.samplemethod == 'sample':
                command = '{}.sample(False, {})'.format(command, sqlquery.samplefraction)
            if sqlquery.maxrows >= 0:
                command = '{}.take({})'.format(command, sqlquery.maxrows)
            else:
                command = '{}.collect()'.format(command)
        command = 'for {} in {}: print({})'.format(LONG_RANDOM_VARIABLE_NAME,
                                                   command,
                                                   LONG_RANDOM_VARIABLE_NAME)
        return command
