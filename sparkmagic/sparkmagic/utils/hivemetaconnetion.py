from hivemetastore.remotehivemeta import RemoteHiveMeta

class HiveMetaStoreConnection:
    def __init__(self, *args):
        self._rhm = RemoteHiveMeta(*args)

    def getDatabases(self):
        return self._rhm.getDatabases().stdout.split()

    def getTables(self, table=None):
        return self._rhm.getTables(table).stdout.split()

    def getDescription(self, database, table):
        return self._rhm.getDescription(table, database).stdout.split()
