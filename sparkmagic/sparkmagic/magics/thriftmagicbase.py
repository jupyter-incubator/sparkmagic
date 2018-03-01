import re
from time import time

import pandas as pd

from IPython.core.magic import Magics, magics_class
from IPython.display import clear_output

from hdijupyterutils.ipythondisplay import IpythonDisplay
from sparkmagic.thriftclient.thriftcontroller import ThriftController
from sparkmagic.thriftclient.querylogs import QueryLogs
from sparkmagic.thriftclient.thriftutils import writeln, send_error, exit_with_none, exit_with_data, time_and_write
from sparkmagic.utils.constants import THRIFT_VAR, THRIFT_LOG_VAR
from sparkmagic.thriftclient.sqlquery import SqlQueries, SqlQuery
from sparkmagic.thriftclient.outputhandler import OutputHandler
from sparkmagic.thriftclient.thriftexceptions import ThriftExecutionError, ThriftMissingKeywordError, ThriftConfigurationError
import sparkmagic.utils.configuration as conf

from pyhive.exc import OperationalError, ProgrammingError
from TCLIService.ttypes import TOperationState
from thrift.transport.TTransport import TTransportException

def interrupt_handle():
    def interrupt_handle_wrapper(f):
        def _interrupt_handle_wrapper(self, *args, **kwargs):
            # Keyboard interrupts
            try:
                return f(self, *args, **kwargs)
            except KeyboardInterrupt as kei:
                self.ipython_display.writeln('Shutting down query...')
                if self.thriftcontroller.cursor:
                    self.thriftcontroller.cursor.cancel()
                    self.thriftcontroller.cursor = None
                self.ipython_display.writeln('DONE')
                clear_output()
                self.ipython_display.writeln('Interrupted query!')
                return None
        return _interrupt_handle_wrapper
    return interrupt_handle_wrapper

@magics_class
class ThriftMagicBase(Magics):
    def __init__(self, shell):
        # You must call the parent constructor
        super(ThriftMagicBase, self).__init__(shell)
        self.shell = shell

        self.ipython_display = IpythonDisplay(shell)

        self.thriftcontroller = ThriftController(self.ipython_display)
        try:
            self.thriftcontroller.locate_connection_details()
        except ThriftConfigurationError as th:
            self.ipython_display.send_error(th.message)
            self.logger.error(th.message)

        self.has_started = False

        self.logger.debug("Initialized '{}'".format(self.__class__))

        # TODO: rewrite as execution contexts
        #self.executioncontexts = ExecutionContexts()

    @interrupt_handle()
    @time_and_write()
    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         output_var, log_var, quiet, coerce):
        # TODO: if async, thread and return
        # Output handlers and logs for query
        # Should be handled outside for all logs using the execution count id
        # self.executioncontexts.add(ExecutionContext(querylogs, outputhandler...))
        querylogs = QueryLogs()
        # Avoid that long running queries fill the entire screen
        outputhandler = OutputHandler(clear_output, ndisplay=30)
        writeln = outputhandler.wrap(self.ipython_display.writeln)
        send_error = outputhandler.wrap(self.ipython_display.send_error)

        # Exluding unicode to avoid non user-friendly internals
        user_variables = SqlQueries.user_variables(self.shell)

        # Create new controller if doesn't exists
        if not self.thriftcontroller.cursor:
            writeln("Establishing new connection to thriftserver...")
            try:
                self.thriftcontroller.connect()
            except ThriftConfigurationError as tce:
                send_error(tce.message)
                self.logger.error(tce.message)
                return
            except Exception as e:
                send_error(e.message)
                self.logger.error(e.message)
                return

        try:
            df = self._execute_sqlquery(cell, samplemethod, maxrows, samplefraction,
                                    output_var, quiet, coerce, user_variables,
                                    writeln, querylogs)
        except ThriftExecutionError as tee:
            send_error(tee.message)
            df = None
        else:
            # Clears ipython text output for active cell
            clear_output()

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
                                  outputhandler.fulllog(),
                                  df,
                                  log_var_name=log_var,
                                  data_var_name=output_var)
        else:
            return exit_with_none(self.shell,
                                  self.ipython_display,
                                  outputhandler.fulllog(),
                                  log_var_name=log_var)




    def _execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         output_var, quiet, coerce, user_variables, writeln, querylogs):

        # Execute each subquery and eliminate empty list entries

        queries = SqlQueries(cell)
        queries.applyargs(samplemethod, maxrows, samplefraction)

        for query_i, query in enumerate(queries):
            t_query = time()
            # Do not use pyhives 'pramater' arguement since it quotes every parameter
            while True:
                try:
                    query.parse(user_variables)
                except KeyError as ke:
                    err = "Could not find variable: '{}'".format(ke.message)
                    err += "\nCurrent user variables:\n{{{}}}".format('\n'.join('{} = {!r}'.format(*item) for item in user_variables.items()))
                    writeln(err)

                    # Python 2/3 compatability
                    try:
                        input = raw_input
                    except NameError:
                        pass
                    self.shell.user_ns[str(ke.message)] = input("{}: ".format(ke.message))
                    user_variables = SqlQueries.user_variables(self.shell)
                    continue
                except ValueError as ve:
                    err = '{}: {}'.format(ve.__class__.__name__, ve.message)
                    err += "\nLocal variables need to be formatted similar to %()s - notice the s only applies to strings"
                    raise ThriftExecutionError(err)
                except TypeError as te:
                    err = '{}: {}'.format(te.__class__.__name__, te.message)
                    err += "\nLocal variables need to be formatted similar to %()s - notice the s only applies to strings"
                    raise ThriftExecutionError(err)
                break

            # Execute query
            try:
                self.thriftcontroller.execute(query, async=True)
            except OperationalError as ope:
                # Returns a TCLIService.ttypes.TExecuteStatementResp
                # Catches query syntax errors
                err = ope.args[0].status.errorMessage
                raise ThriftExecutionError(err)
            except TTransportException as tte:
                err = '{}: {}'.format(tte.__class__.__name__,tte.message)
                err += "\nPossible issues in order: {}".format(''.join(['\n-> {}']*2)).format(
                            "Connection to destination host is down",
                            "Thriftserver is not running"
                        )
                self.thriftcontroller.cursor = None
                raise ThriftExecutionError(err)

            # for creating progressbars
            has_started = False
            querylogs + query
            writeln(query)

            # Wait for query to finish printing status updates
            try:
                # Check for query udates
                state = self.thriftcontroller.cursor.poll()
                status = state.operationState

                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    logs = self.thriftcontroller.cursor.fetch_logs()
                    querylogs + logs
                    for message in logs:
                        writeln(message)

                    if 'map' in ''.join(logs).lower():
                        has_started = True

                    t_current = time()-t_query
                    # Ping a potentially impatiant user
                    if (not has_started) and t_current > 15:
                        writeln(" ".join([
                                "Query is taking unusally long to start (waited {:.2f}s).".format(t_current),
                                "Hadoop cluster memory or vcores might be saturated.",
                                "Shutdown with interrupt (shortcut i,i in command mode)."
                            ]))
                    status = self.thriftcontroller.cursor.poll().operationState
            except TTransportException as tte:
                err = '{}: {}'.format(tte.__class__.__name__, tte.message)
                err += "\nPossible issues in order: {}".format(''.join(['\n-> {}']*2)).format(
                            "Connection to destination host is down",
                            "Thriftserver is not running"
                        )
                self.thriftcontroller.cursor = None
                raise ThriftExecutionError(err)
            writeln("Query execution time: {:.2f}s\n".format(time() - t_query))

            # Parse output for each query (intermediate results are printed to screen)
            try:
                headerdesc = self.thriftcontroller.cursor.description
            except OperationalError as ope:
                # Catches query syntax errors
                # Returns a TCLIService.ttypes.TExecuteStatementResp
                err = ope.args[0].status.errorMessage
                err += "\nPossible issues in order: {}".format(''.join(['\n-> {}']*4)).format(
                            "Query was killed by resourcemanager",
                            "Query was killed by a user",
                            "Connection dropped during execution",
                            "User could not be authenticated (i.e. bad/wrong username/password)"
                        )
                err += "\n{}\n{}".format(
                            "Note that 'username' must match your workbench 'username'",
                            "If running jupyter as a service on remote machines 'username' should be set manually"
                        )
                raise ThriftExecutionError(err)

            # Some queries does not return any data or description
            if headerdesc:
                # If fetching rows still fails -> likely a bug
                try:
                    records = self.thriftcontroller.cursor.fetchall()
                except ProgrammingError as pre:
                    err = "{}\n{}\n{}".format(
                                str(pre),
                                "Likely a missing client side parser issue.",
                                "Consider filing a bug..."
                            )
                    raise ThriftExecutionError(err)

                # Create a dataframe from the output
                header = [desc[0].split('.')[-1] for desc in headerdesc]
                types = [desc[1] for desc in headerdesc]

                df = pd.DataFrame.from_records(records, columns=header)
                # Last query does not display potential outputs, but returns dataframe for viz
                # Might want to save this output elsewhere in the future
                if query_i < len(queries)-1:
                    writeln(df)

        # Notify user that last query did not return any data
        if not headerdesc:
            writeln("Query did not return any data")
            return None

        # If the user don't want any results
        if quiet:
            return None
        else:
            return df
