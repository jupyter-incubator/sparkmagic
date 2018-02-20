import re

from sparkmagic.utils.hivekeywords import HIVE_KEYWORDS
from sparkmagic.utils.trie import TrieNode, Trie

from hivemetastore.remotehivemeta import RemoteHiveMeta

class Completer:
    def __init__(self):
        self._suggestions = []

        # All hive keyword suggestions
        hktree = Trie()
        for key in HIVE_KEYWORDS:
            hktree.add(key)
        self._hktree = hktree

        # Keep all databases in memory
        remote_hivemetastore = RemoteHiveMeta()
        databases = remote_hivemetastore.getDatabases()
        dbtree = Trie()
        for database in databases:
            dbtree.add(database)
        self._dbtree = dbtree
        self.remote_hivemetastore = remote_hivemetastore

    def _nullify(self):
        self._prefix = ""
        self._wordspan = (0, 0)
        self._suggestions = []

    def complete(self, code, pos):
        wordsincode = list(re.finditer(r'(\w+)', code))
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

        # Cursor postion was not in 'good' place for completion
        if not wordtocomplete:
            self._nullify()
            return False

        prefix = wordtocomplete[0:pos-wordspan[0]]

        # Use trie structure to grab hive matches
        hksuggestions = self._hktree.find_prefix(prefix.upper())

        # Add from metadata here
        suggestions = hksuggestions

        self._prefix = prefix
        self._wordspan = wordspan
        self._suggestions = suggestions
        return True

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
    completer.complete("select * from whe limit 5", 15)
    matches = completer.suggestions()
    prefix = completer.prefix()
    (start_pos, end_pos) = completer.cursorpostitions()
    print(prefix)
    print("Start: {}, End: {}".format(start_pos, end_pos))
    print(matches)
