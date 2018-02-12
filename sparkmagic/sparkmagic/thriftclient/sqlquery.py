import re

class SqlQueries:
    @staticmethod
    def user_variables(shell):
        accepted_types = [str, float, int, bool]
        # Hacky way of guessing a the users variables (avoid built-ins etc...)
        return {key:var for key,var in shell.user_ns.items() if type(var) in accepted_types and key[0]!='_'}

    def __init__(self, queries):
        # Elimiate empty list entries and strip newlines (newlines can cause queries to silently not execute)
        self.queries = [SqlQuery(q.strip()) for q in cell.split(';') if q]

    def applyargs(self, samplemethod, maxrows, samplefraction):
        if samplefraction:
            # This will be handled once everything is returned
            # TODO: make this in hive/spark
            return
        if samplemethod:
            if samplemethod=='sample':
                self.queries[-1] = "{}\DISTRIBUTE BY rand() SORT BY rand()".format(self.queries[-1])
            elif samplemethod=='take':
                pass:
            else:
                raise ValueError("Unrecognized samplemethod: {}".format(samplemethod))

            # Need to make sure maxrows has a value if samplemethod is set
            if not maxrows:
                maxrows = 10

        if maxrows:
            self.queries[-1] = "{}\nLIMIT {}".format(self.queries[-1], maxrows)
        raise NotImplementedError

        def __iter__(self):
            for query in self.queries:
                yield query

class SqlQuery:
    def __init__(self, query):
        self.query = query

    def parse(parameters):
        if parameters:
            # Hack to avoid % format conflicts - replace all % with %%
            query_mod = re.sub(r'%([^(])', r'%%\1', self.query)
            self.query = query_mod % parameters

    def __str__(self):
        return str(self.query)
