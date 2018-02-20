from __future__ import print_function
from calljava import CallJava 

class RemoteHiveMeta:
    def __init__(self, hivexml):
        self._remote = CallJava(hivexml)

    def getDatabases(self):
        return self._remote.calljava("D:all")

    def getTables(self, table=None):
        if not table:
            return self._remote.calljava("T:all")
        else:
            return self._remote.calljava("D:spec", table)

    def getDescription(self, database, table):
        return self._remote.calljava("T:spec", table, database)

if __name__ == '__main__':
    rm = RemoteHiveMeta()
    print(rm.getDatabases())
