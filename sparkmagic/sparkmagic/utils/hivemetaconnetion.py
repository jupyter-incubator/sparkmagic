from hivemetastore.remotehivemeta import RemoteHiveMeta

class HiveMetaStoreConnection:
    def __init__(self, *args):
        self._rhm = RemoteHiveMeta(*args)

    def getDatabases(self):
        return self._rhm.getDatabases().stdout.split()

    def getTables(self, database=None, pattern=None):
        return self._rhm.getTables(database, pattern).stdout.split()

    def getDescription(self, database, table):
        return self._rhm.getDescription(table, database).stdout.split()
