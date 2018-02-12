from IPython.core.magic import Magics, magics_class
from IPython.display import clear_output

from sparkmagic.utils.sparklogger import SparkLog
from hdijupyterutils.ipythondisplay import IpythonDisplay
from sparkmagic.thriftclient.thriftcontroller import ThriftController
from sparkmagic.thriftclient.querylogs import QueryLogs
from sparkmagic.thriftclient.thriftutils import writeln, send_error, exit_with_none, exit_with_data, time_and_write
from sparkmagic.utils.constants import THRIFT_VAR, THRIFT_LOG_VAR
from sparkmagic.thriftclient.sqlquery import SqlQueries, SqlQuery

from pyhive.exc import OperationalError, ProgrammingError
from TCLIService.ttypes import TOperationState
from thrift.transport.TTransport import TTransportException

import pandas as pd

import re
from time import time

@magics_class
class ThriftMagicBase(Magics):
    def __init__(self, shell):
        # You must call the parent constructor
        super(ThriftMagicBase, self).__init__(shell)
        self.shell = shell

        self.logger = SparkLog(u"ThriftMagics")
        self.ipython_display = IpythonDisplay()

        self.thriftcontroller = ThriftController(self.ipython_display)
        self.has_started = False

        self.querylogs = QueryLogs()
        self.writeln = writeln(self.querylogs, self.ipython_display)
        self.send_error = send_error(self.querylogs, self.ipython_display)

    @time_and_write()
    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         output_var, log_var, quiet, coerce):
        # if async, thread and return

        # Keyboard interrupts
        try:
            # Exluding unicode to avoid non user-friendly internals
            user_variables = SqlQueries.user_variables(self.shell)

            # Clears internal log structure
            self.querylogs.clear()

            # Create new controller if doesn't exists
            if not self.thriftcontroller.cursor:
                self.writeln("Establishing new connection to thriftserver...")
                if not self.thriftcontroller.connect():
                    return None


            df = self._execute_sqlquery(cell, samplemethod, maxrows, samplefraction,
                                        session, output_var, quiet, coerce)
            # Clears the ipython text
            clear_output()
        except KeyboardInterrupt as kei:
            self.writeln('Shutting down query...')
            if self.thriftcontroller.cursor:
                self.thriftcontroller.cursor.cancel()
                self.thriftcontroller.cursor = None
            self.writeln('DONE')
            clear_output()
            self.writeln('Interrupted query')
            df = None

        # Save the query runtime output for review
        if df is not None:
            # Remaining sampling logic
            if samplefraction:
                if maxrows:
                    df = df.sample(n=maxrows)
                if samplemethod == 'sample':
                    df = df.sample(frac=samplefraction)
                elif samplemethod == 'take':
                    df = df.take(samplefraction*len(df.index))

            return exit_with_data(self.shell,
                                  self.ipython_display,
                                  self.querylogs,
                                  df,
                                  log_var_name=log_var,
                                  data_var_name=output_var)
        else:
            return exit_with_none(self.shell,
                                  self.ipython_display,
                                  self.querylogs,
                                  log_var_name=log_var)



    def _execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         output_var, quiet, coerce):



        # Execute each subquery and eliminate empty list entries

        queries = SqlQueries(cell)
        queries.applyargs(samplemethod, maxrows, samplefraction)

        for query_i, query in enumerate(queries):
            t_query = time()
            # Do not use pyhives 'pramater' arguement since it quotes every parameter
            try:
                query.parse(user_variables)
            except KeyError as ke:
                self.send_error("Could not find variable: '{}'".format(str(ke.message)))
                self.send_error(
                        "Current user variables:\n{{{}}}".format('\n'.join('{} = {!r}'.format(*item) for item in user_variables.items()))
                    )
                return None
            except ValueError as ve:
                self.send_error('{}: {}'.format(ve.__class__.__name__, ve.message))
                self.send_error("Local variables need to be formatted similar to %()s - notice the s only applies to strings")
                return None
            except TypeError as te:
                self.send_error('{}: {}'.format(te.__class__.__name__, te.message))
                self.send_error("Local variables need to be formatted similar to %()s - notice the s only applies to strings")
                return None

            # Execute query
            try:
                self.querylogs + query
                self.thriftcontroller.execute(query, async=True)
            except OperationalError as ope:
                # Returns a TCLIService.ttypes.TExecuteStatementResp
                # Catches query syntax errors
                msg = ope.args[0].status.errorMessage
                self.ipython_display.send_error(msg)
                return None
            except TTransportException as tte:
                self.send_error('{}: {}'.format(tte.__class__.__name__,tte.message))
                self.send_error(
                    "Possible issues in order: {}".format(''.join(['\n-> {}']*2)).format(
                            "Connection to destination host is down",
                            "Thriftserver is not running"
                        )
                    )
                self.thriftcontroller.cursor = None
                return None
            
            # for creating progressbars
            self.has_started = False
            self.ipython_display.writeln(query)

            # Wait for query to finish printing status updates
            try:
                # Check for query udates
                state = self.thriftcontroller.cursor.poll()
                status = state.operationState

                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    logs = self.thriftcontroller.cursor.fetch_logs()
                    self.querylogs + logs
                    for message in logs:
                        self.ipython_display.writeln(message)

                    if 'map' in ''.join(logs).lower():
                        self.has_started = True

                    t_current = time()-t_query
                    if (not self.has_started) and t_current > 10:
                        self.writeln(
                                "Query is taking unusally long to start (waited {:.2f}s). Hadoop cluster memory or vcores might be saturated. Shutdown with interrupt (shortcut i,i in command mode).".format(t_current)
                            )
                    status = self.thriftcontroller.cursor.poll().operationState
            except TTransportException as tte:
                self.send_error('{}: {}'.format(tte.__class__.__name__,tte.message))
                self.send_error(
                    "Possible issues in order: {}".format(''.join(['\n-> {}']*2)).format(
                            "Connection to destination host is down",
                            "Thriftserver is not running"
                        )
                    )
                self.thriftcontroller.cursor = None
                return None
            self.writeln("Query execution time: {:.2f}s\n".format(time() - t_query))

            # Parse output for each query (intermediate results are printed to screen)
            try:
                headerdesc = self.thriftcontroller.cursor.description
            except OperationalError as ope:
                # Catches query syntax errors
                # Returns a TCLIService.ttypes.TExecuteStatementResp
                msg = ope.args[0].status.errorMessage
                self.send_error(msg)
                self.send_error(
                    "Possible issues in order: {}".format(''.join(['\n-> {}']*4)).format(
                            "Query was killed by resourcemanager",
                            "Query was killed by a user",
                            "Connection dropped during execution",
                            "User could not be authenticated (i.e. bad/wrong username)"
                        )
                    )
                self.send_error(
                    "{}\n{}".format(
                            "Note that 'username' must match your workbench 'username'",
                            "If running jupyter as a service on remote machines 'username' should be set manually"
                        )
                    )
                return None

            # Some queries does not return any data or description
            if headerdesc:
                # If fetching rows still fails -> likely a bug
                try:
                    records = self.thriftcontroller.cursor.fetchall()
                except ProgrammingError as pre:
                    self.send_error(str(pre))
                    self.send_error("Likely a missing client side parser issue.")
                    self.send_error("Consider filing a bug...")
                    return None

                # Create a dataframe from the output
                header = [desc[0].split('.')[-1] for desc in headerdesc]
                types = [desc[1] for desc in headerdesc]

                df = pd.DataFrame.from_records(records, columns=header)
                # Last query does not display potential outputs, but returns dataframe for viz
                if query_i < len(queries)-1:
                    self.ipython_display.writeln(df)

        # Notify user that last query did not return any data
        if not headerdesc:
            self.writeln("Query did not return any data")
            return None

        # If the user don't want any results
        if quiet:
            return None
        else:
            return df
