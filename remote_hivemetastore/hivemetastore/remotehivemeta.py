from __future__ import print_function
from hivemetastore.calljava import CallJava 
from hivemetastore.metaexceptions import JavaCallException

def raise_bash_exception(f):
    def wrapper(*args):
        bash_output = f(*args)
        stderr = bash_output.stderr.lower()
        if "error" in str(stderr) or "exception" in str(stderr):
            raise JavaCallException(bash_output.stderr)
        return bash_output
    return wrapper

class RemoteHiveMeta:
    def __init__(self, hivexml, timeout=600):
        self._remote = CallJava(hivexml, timeout)

    @raise_bash_exception
    def getDatabases(self):
        return self._remote.calljava("D:all")

    @raise_bash_exception
    def getTables(self, database=None, pattern=None):
        # Get all tables for entire database
        if not database:
            return self._remote.calljava("T:all")
        elif not pattern:
            return self._remote.calljava("D:spec", database)
        else:
            return self._remote.calljava("T:pttn", database, pattern)

    @raise_bash_exception
    def getDescription(self, database, table):
        return self._remote.calljava("T:spec", table, database)

    def __repr__(self):
        return '{!r}(_remote={})'.format(self.__class__, self._remote)

import sys
if __name__ == '__main__':
    rm = RemoteHiveMeta(sys.argv[1])
    try:
        bash_output = rm.getDatabases()
    except JavaCallException as e:
        print("Failed with:", e, sep='\n')
    else:
        print("SUCCESS!")
        print(bash_output)
