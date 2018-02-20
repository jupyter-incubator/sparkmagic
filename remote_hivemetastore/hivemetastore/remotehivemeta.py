from __future__ import print_function
from calljava import CallJava 

class RemoteHiveMeta:
    def __init__(self, hivexml, timeout=600):
        self._remote = CallJava(hivexml, timeout)

    def getDatabases(self):
        return self._remote.calljava("D:all")

    def getTables(self, database=None, pattern=None):
        # Get all tables for entire database
        if not database:
            return self._remote.calljava("T:all")
        elif not pattern:
            return self._remote.calljava("D:spec", database)
        else:
            return self._remote.calljava("T:pttn", database, pattern)

    def getDescription(self, database, table):
        return self._remote.calljava("T:spec", table, database)

if __name__ == '__main__':
    rm = RemoteHiveMeta()
    print(rm.getDatabases())
