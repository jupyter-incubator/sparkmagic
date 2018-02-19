# To get jars run with -verbose:class | grep apache > alljars

from __future__ import print_function
from collections import deque

# Assumes Classes are at leaf nodes
# This has significance when if adding for instance org.x.y.z.A and org.x.B
class DomainTree:
    def __init__(self, separator="."):
        self._separator = separator
        self._root = Node("_")

    def addnode(self, name):
        fullpath = name.split(".")
        classname = fullpath[-1]
        path = deque(fullpath[0:-1])
        if not path or not classname:
            return False
        else:
            self._addnode(self._root, path, classname)
            return True

    # Possibilities
    # Path is empty after removing last domainlevel -> add class to classlist
    # Path exists in current nodes domain pointer -> recursive call
    # Path does not exist -> create new Node and recrusive call
    def _addnode(self, node, path, classname):
        level = path.popleft()

        # Add class
        if not path:
            if classname not in node.classes:
                node.classes.append(classname)
                return True
        elif node.domains.get(level, None) is not None:
            return self._addnode(node.domains[level], path, classname)
        else:
            node.domains[level] = Node(level)
            return self._addnode(node.domains[level], path, classname)
        
        return False

    def getcommonpackage(self):
        allbases = []
        partialname = []
        self._getcommonpackage(self._root, allbases, partialname)
        return allbases

    def _getcommonpackage(self, node, allbases, partialname):
        if node.classes:
            allbases.append(self._separator.join(partialname))
        else:
            for name, childnode in node.domains.items():
                partialname.append(name)
                self._getcommonpackage(childnode, allbases, partialname)
                partialname.pop()

class Node:
    def __init__(self, name):
        self.name = name
        self.classes = []
        self.domains = {}

    def __str__(self):
        return "Domaintree node: {}".format(self.name)
