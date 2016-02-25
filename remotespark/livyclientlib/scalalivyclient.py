# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from .livyclient import LivyClient


class ScalaLivyClient(LivyClient):
    """Spark client for Livy session in Scala"""

    def _get_command_for_query(self, sqlquery):
        command = 'sqlContext.sql("""{}""")'.format(sqlquery.query)
        if sqlquery.only_columns:
            command = '{}.columns'.format(command)
        else:
            command = '{}.toJSON'.format(command)
            if sqlquery.samplemethod == 'sample':
                command = '{}.sample(false, {})'.format(command, sqlquery.samplefraction)
            if sqlquery.maxrows >= 0:
                command = '{}.take({})'.format(command, sqlquery.maxrows)
            else:
                command = '{}.collect'.format(command)
        return '{}.foreach(println)'.format(command)

