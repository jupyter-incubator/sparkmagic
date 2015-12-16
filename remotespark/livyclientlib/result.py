"""A set of classes which denote abstract "results" that are returned by the
Livy session. A Result can represent a string of output that should be sent
to Jupyter's stdout or stderr stream. A Result can also be a dataframe 
which Jupyter renders directly itself. We may add more Result types later
if there is a need. 

Every Result has a "render" method which accepts an IPython kernel as its 
input and returns the data associated with the Result that the kernel should
display. The render method may also have the side effect of writing to stdout
or stderr.

Every result can also be converted to a string with the str function."""

class Result(object):
    def render(self, shell):
        raise NotImplementedError("Render method must be overridden by subclass")

    def __str__(self):
        raise NotImplementedError("str function must be overridden by subclass")

class SuccessResult(Result):
    def __init__(self, s):
        self.success = s

    def __str__(self):
        return self.success

    def render(self, shell):
        shell.write(self.success)
        return None

class ErrorResult(Result):
    def __init__(self, s):
        self.error = s

    def __str__(self):
        return self.error
        
    def render(self, shell):
        shell.write_err(self.error)
        return None

class DataFrameResult(Result):
    def __init__(self, df):
        self.df = df

    def __str__(self):
        return str(self.df)
        
    def render(self, shell):
        return self.df
