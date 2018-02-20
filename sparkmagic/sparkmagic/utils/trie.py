class TrieNode(object):
    def __init__(self, char):
        self.char = char
        self.children = []
        self.word_finished = False
        # How many times this character appeared in the addition process
        self.counter = 1

class Trie(object):
    def __init__(self):
        self._root = TrieNode("")

    def add(self, word):
        node = self._root
        for char in word:
            found_in_child = False
            # Search for the character in the children of the present `node`
            for child in node.children:
                if child.char == char:
                    # We found it, increase the counter by 1 to keep track that another
                    # word has it as well
                    child.counter += 1
                    # And point the node to the child that contains this char
                    node = child
                    found_in_child = True
                    break
            # We did not find it so add a new chlid
            if not found_in_child:
                new_node = TrieNode(char)
                node.children.append(new_node)
                # And then point node to the new child
                node = new_node
        # Everything finished. Mark it as the end of a word.
        node.word_finished = True


    def find_prefix(self, prefix):
        node = self._root
        matches = []
        if not node.children:
            return matches
        for char in prefix:
            char_not_found = True
            # Search through all the children of the present `node`
            for child in node.children:
                if child.char == char:
                    char_not_found = False
                    # Assign node as the child containing the char and break
                    node = child
                    break
            # Return False anyway when we did not find a char.
            if char_not_found:
                return matches

        # Grab all strings after the 'node'
        tmp_match = [prefix[:-1]]
        self.getall(node, tmp_match, matches)
        return matches

    def getall(self, node, tmp_match, matches):
        if node.word_finished:
            matches.append("".join(tmp_match + [node.char]))
        for child in node.children:
            tmp_match.append(node.char)
            self.getall(child, tmp_match, matches)
            tmp_match.pop()
