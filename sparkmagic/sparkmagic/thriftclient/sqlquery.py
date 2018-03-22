import re
from sparkmagic.thriftclient.thriftutils import DefaultVar
from collections import namedtuple

TemplateVar = namedtuple('TemplateVar', ['template', 'defaultvar'])

class SqlQueries:
    @staticmethod
    def user_variables(shell):
        accepted_types = [str, float, int, bool]
        # Hacky way of guessing a the users variables (avoid built-ins etc...)
        return {key:var for key,var in shell.user_ns.items() if type(var) in accepted_types and key[0]!='_'}

    def __init__(self, queries):
        # Elimiate empty list entries and strip newlines (newlines can cause queries to silently not execute)
        self._raw_querytext = queries
        self._post_template_queries = None
        self.queries = [SqlQuery(q.strip()) for q in queries.split(';') if q]
        self._templatevars = None


    def post_template_queries(self):
        return self._post_template_queries

    def get_defaultvars(self):
        return [var.defaultvar for var in self._templatevars]

    def parse_templatevars(self):
        templatestr = re.findall(r'\$<([^>]+)>', self._raw_querytext)
        templatevars = []

        for template in templatestr:
            # Spearte key and default with = sign
            vs = template.split("=", 1)
            if len(vs) == 1:
                templatevars.append(TemplateVar(template, DefaultVar(vs[0], '')))
            else:
                templatevars.append(TemplateVar(template, DefaultVar(*vs)))

        self._templatevars = templatevars


    def replace_templatevars(self, namedvars):
        """
            Creates an internal text representation of the queries with template vars filled in
            Assumes that namedvars are inputted in the same order as templates should be replaced
        """
        if len(namedvars) != len(self._templatevars):
            raise ThriftSQLException("Replace templatevars need as many template variables as input variables")
        parsed_query = self._raw_querytext
        for tvar, nvar in zip(self._templatevars, namedvars):
            parsed_query = re.sub(r'\$<{}>'.format(tvar.template), nvar.value, parsed_query, 1)

        self._post_template_queries = parsed_query


    def has_templatevars(self):
        return bool(self._templatevars)

    def applyargs(self, samplemethod, maxrows, samplefraction):
        if samplefraction:
            # This is handled once results are returned
            # TODO: implement this through modify sql query
            return
        if samplemethod:
            if samplemethod=='sample':
                self.queries[-1] = SqlQuery("{}\DISTRIBUTE BY rand() SORT BY rand()".format(self.queries[-1]))
            elif samplemethod=='take':
                pass
            else:
                raise ValueError("Unrecognized samplemethod: {}".format(samplemethod))

            # Need to make sure maxrows has a value if samplemethod is set
            if not maxrows:
                maxrows = 10

        if maxrows:
            self.queries[-1] = SqlQuery("{}\nLIMIT {}".format(self.queries[-1], maxrows))

    def __iter__(self):
        for query in self.queries:
            yield query

    def __len__(self):
        return len(self.queries)

class ThriftSQLException(Exception):
    pass

class SqlQuery:
    def __init__(self, query):
        self.query = query

    def parse(self, parameters=None):
        if parameters is not None:
            # Hack to avoid % format conflicts - replace all % with %%
            query_mod = re.sub(r'%([^(])', r'%%\1', self.query)
            self.query = query_mod % parameters

    def __str__(self):
        return str(self.query)
