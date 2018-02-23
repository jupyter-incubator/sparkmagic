import re

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.hivekeywords import HIVE_KEYWORDS
from sparkmagic.utils.trie import TrieNode, Trie
from sparkmagic.utils.configuration import hive_xml, metastore_timeout
from sparkmagic.utils.hivemetaconnetion import HiveMetaStoreConnection

from hivemetastore.metaexceptions import TimeOutException
from hivemetastore.metaexceptions import JavaCallException


class Completer:
    def __init__(self):
        self.logger = SparkLog(u"Completer", conf.logging_config_debug())

        self._suggestions = []

        import sys
        # All hive keyword suggestions
        hktree = Trie()
        for key in HIVE_KEYWORDS:
            hktree.add(key)
        self._hktree = hktree

        # Keep all databases in memory
        # But if metastore is unreachable time out and do not use
        usehivemeta = True
        remote_hivemetastore = HiveMetaStoreConnection(hive_xml(), metastore_timeout())
        try:
            databases = remote_hivemetastore.getDatabases()
        except TimeOutException as e:
            self.logger.warn("Timedout waiting for databases")
            self.logger.warn("Skipping HIVE metastore keywords...")
            usehivemeta = False
            dbtree = None
            remote_hivemetastore = None
        except JavaCallException as e:
            self.logger.warn("Failed executing query with:\n{}".format(e))
            self.logger.warn("Skipping HIVE metastore keywords...")
            usehivemeta = False
            dbtree = None
            remote_hivemetastore = None

        if usehivemeta:
            dbtree = Trie()
            self.logger.debug("Adding {} databases ({}...)".format(len(databases), databases[:3]))
            for database in databases:
                dbtree.add(database)

        self._dbtree = dbtree
        self._remote_hivemetastore = remote_hivemetastore

        self.logger.debug("Initialized Completer")

    def _nullify(self):
        self._prefix = ""
        self._wordspan = (0, 0)
        self._suggestions = []

    def complete(self, code, pos):
        wordsincode = list(re.finditer(r'([a-zA-Z0-9_\-.]+)', code))
        # No input words -> nothing to do
        if not wordsincode:
            self._nullify()
            return False

        # Find which word cursor is in
        wordtocomplete = None
        for match in wordsincode:
            if self.inbetween(pos, match.span()):
                wordspan = match.span()
                wordtocomplete = match.group(0)
                break

        self._fullword = wordtocomplete
        self.logger.debug("Word to complete: {}".format(wordtocomplete))

        # Cursor postion was not in 'good' place for completion
        if not wordtocomplete:
            self._nullify()
            return False

        prefix = wordtocomplete[0:pos-wordspan[0]]
        self.logger.debug("Found prefix: {}".format(prefix))

        # Use trie structure to grab hive matches
        self.logger.debug("Looking for prefix in hive keywords...")
        hksuggestions = self._hktree.find_prefix(prefix)

        # Add from metadata here
        hmtables = []
        hmdatabases = []
        if self._remote_hivemetastore:
            self.logger.debug("Looking for prefix in hive metastore...")

            # Try to guess for databases or tables
            db_tb = wordtocomplete.split(".")
            # We are looking for a table and the position is in the 'table' area
            if len(db_tb) > 1 and pos > len(db_tb[0])+wordspan[0]:
                self.logger.debug("Looking for prefix in hive tables...")
                tableprefix = db_tb[1][0:pos-wordspan[0]+len(db_tb[1])] + "*"
                self.logger.debug("Using {!r} from {}".format(tableprefix, db_tb))
                try:
                    tables = self._remote_hivemetastore.getTables(db_tb[0], tableprefix)
                except TimeOutException as e:
                    self.logger.warn("Timedout waiting for tables")
                    self.logger.warn("Skipping HIVE table keywords...")
                except JavaCallException as e:
                    self.logger.warn("Failed to retrieve tables with:\n{}".format(e))
                    self.logger.warn("Skipping HIVE table keywords...")

                self.logger.debug("Found {} tables ({}...)".format(len(tables), tables[:3]))
                hmtables = ["{}.{}".format(db_tb[0],t) for t in tables]
            hmdatabases = self._dbtree.find_prefix(prefix)
            self.logger.debug("Found {} databases ({}...)".format(len(hmdatabases), hmdatabases[:3]))

        suggestions = hmtables + hmdatabases + hksuggestions

        self._prefix = prefix
        self._wordspan = wordspan
        self._suggestions = suggestions
        return True

    def fullword(self):
        return self._fullword

    def suggestions(self):
        return self._suggestions

    def cursorpostitions(self):
        return self._wordspan

    def prefix(self):
        return self._prefix

    # Note the range is right inclusive, due to possible following whitepsace
    # 0 id of each word is not matched
    @staticmethod
    def inbetween(num, span):
        return num > span[0] and num <= span[1]

if __name__ == '__main__':
    completer = Completer()
    completer.complete("select * from cluster_metrics_prod_2.c where limit 5", 38)
    matches = completer.suggestions()
    prefix = completer.prefix()
    (start_pos, end_pos) = completer.cursorpostitions()
    print(prefix)
    print("Start: {}, End: {}".format(start_pos, end_pos))
    print(matches)
