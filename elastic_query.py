import requests
import re
import json
import ast


class ElasticQuery:

    HEADERS = {
        'Content-Type': 'application/json'
    }

    def __init__(self, url, index=None, _type=None, size=None):
        self.url = url
        self.index = index
        self.type = _type
        self.query = None
        self.fields = []
        self.query_size = size or 10  # Mirrors Elasticsearch's default
        self.request = None
        self.response = None
        self.results = []

    def _build_querystring_query(self, query_term):
        return {"query_string": {"query": query_term}}

    def _build_query_match(self, field_name, value):
        return {"match": {field_name: value}}

    def _build_query_match_phrase(self, field_name, value):
        #TODO:  Add option handling, example, slop
        return {"match_phrase": {field_name: value}}

    def _parse_single_query_term(self, query_term):
        """
        Parses single query expression into Elasticsearch query DSL

        :param query_term: query expression to be parsed
        :return: two element tuple containing the operation and the parsed query expression
        """
        operators = ["~=", "!~"]
        match_pattern = "([\w-]+)({match}|{not_match})([\w-]+)|([\w-]+)({match}|{not_match})'([\w-]+)'"
        match_pattern = match_pattern.format(match=operators[0], not_match=operators[1])
        single_term_match = re.match(match_pattern, query_term)
        if single_term_match:
            unquoted = single_term_match.groups()[:3]
            quoted = single_term_match.groups()[3:]
            match = unquoted if unquoted[0] else quoted
            field_name = match[0]
            operation = match[1]
            value = match[2]
            return operation, self._build_query_match(field_name, value)

        phrase_pattern = "([\w-]+)(~=)'([\w-]+\s+[\w-]+(?:\s+[\w-]+)*)'$"
        phrase_match = re.match(phrase_pattern, query_term)
        if phrase_match:
            match = phrase_match.groups()
            field_name = match[0]
            operation = match[1]
            value = match[2]
            return operation, self._build_query_match_phrase(field_name, value)
        return {}, {}

    @staticmethod
    def map_operation(ast_op, query_op):
        if ast_op == "and":
            if query_op == "~=":
                return "must"
            elif query_op == "!~":
                return "must_not"
            return "must"
        if ast_op == "or":
            return "should"

    def resolve_ast(self, query_ast):
        """
        Translate AST representation of the search query into an Elasticsearch query

        :param query_ast: AST of input query expression
        :return: Elasticsearch query ("query" portion of the whole query)
        """
        if type(query_ast) is str:
            return self._parse_single_query_term(json.dumps(query_ast).strip('"'))[1]
        query_ast = dict(query_ast)
        ast_operation = list(query_ast.keys())[0]  # Root node of the AST
        query_operations = [None, None]
        operands = list(query_ast.values())[0]

        for i, operand in enumerate(operands):
            if type(operand) is str:
                query_operations[i], operands[i] = self._parse_single_query_term(json.dumps(operand).strip('"'))
            else:
                # if operand is an object call recursively
                operands[i] = self.resolve_ast(operand)

        left_query_operation = ElasticQuery.map_operation(ast_operation, query_operations[0])
        right_query_operation = ElasticQuery.map_operation(ast_operation, query_operations[1])

        # If operations are different (i.e. AND and OR) separate members are needed
        # (i.e must and should)
        if left_query_operation != right_query_operation:
            result = {
                "bool": {
                    left_query_operation: [
                        operands[0]
                    ],
                    right_query_operation: [
                        operands[1]
                    ]
                }
            }
        # Otherwise group operands under the same operation clause (i.e. both terms of a must)
        else:
            result = {
                "bool": {
                    left_query_operation: [
                        operands[0],
                        operands[1]
                    ]
                }
            }
        return result

    def _parse_search_query_terms(self, query_body):
        """
        Parses the search query expression
        An AST (Abstract Syntax Tree) is used to parse the expression in order
        to handle operation order and grouping (done using parenthesis)
        The patterns defined (using regexes) are passed as a set to the ast function
        to define the tokens

        :param query_body:
        :return: query expression parsed into AST
        """
        operators = ["~=", "!~"]  #TODO: add filter operators: >, <, <=, >=
        match_pattern = "([\w-]+)({match}|{not_match})([\w-]+)|([\w-]+)({match}|{not_match})'([\w-]+)'"
        match_pattern = match_pattern.format(match=operators[0], not_match=operators[1])
        phrase_pattern = "([\w-]+)(~=)'([\w-]+\s+[\w-]+(?:\s+[\w-]+)*)'"
        patterns = [match_pattern, phrase_pattern]

        query_ast = ast.parse_into_ast(query_body, patterns)
        resolved_query_ast = self.resolve_ast(query_ast)
        return resolved_query_ast

    def _parse_query_expression(self, query_expression):
        """
        Parses the expression into search and agg parts

        :param query_expression: Raw query expression from client code
        :return: two element tuple containg the parsed search and agg queries
        """
        search_expression, *agg_expression = re.split("\|", query_expression)
        # If the search expression is just a keyword build a querystring query
        keyword_pattern = "^[\w_-]+\s*$"
        keyword_match = re.match(keyword_pattern, search_expression)
        if keyword_match:
            return self._build_querystring_query(search_expression), {}

        search_query_parsed = self._parse_search_query_terms(query_expression)
        agg_query_parsed = None  #TODO: placeholder

        if search_query_parsed:
            return search_query_parsed, agg_query_parsed
        raise Exception("Invalid Query: {}".format(query_expression))

    def _build_query(self, query_expression):
        search_query, agg_query = self._parse_query_expression(query_expression)
        self.query = {"query": search_query, "size": self.query_size}
        if self.fields:
            self.query["_source"] = self.fields
        if agg_query:
            self.query["aggregations"] = agg_query

    def search(self, query):
        """ Searches """
        try:
            self._build_query(query)
        except Exception as e:
            print(e)
            raise QueryBuildException(e)

        search_url = "{}/{}/{}/_search".format(self.url, self.index, self.type)
        self.response = requests.get(search_url,
                                     data=json.dumps(self.query),
                                     headers=ElasticQuery.HEADERS)
        self._parse_response()

    def _parse_response(self):
        response = self.response.json()
        #total_matches = response["hits"]["total"]
        total_matches_returned = len(response["hits"]["hits"])
        if total_matches_returned > 0:
            results = response["hits"]["hits"]
            for result in results:
                _id = result["_id"]
                doc = result["_source"]
                if self.fields:  #TODO: validate submitted fields
                    entry = dict([(field, doc[field]) for field in self.fields])
                else:
                    entry = doc
                #TODO: provide option to use another field as id
                entry["id"] = _id
                self.results.append(entry)

    def show_instance_info(self):
        self.response = requests.get(self.url)
        self.request = self.response.request
        stats = self.response.json()
        del stats["tagline"]
        print(json.dumps(stats, indent=2))

    def print_query(self):
        print(json.dumps(self.query, indent=2))

    def print_response_summary(self, fields):
        if self.response and len(fields) > 0:
            res = self.response.json()
            hit_count = res["hits"]["total"]
            results = []
            for result in res["hits"]["hits"]:
                entry = []
                for field in fields:
                    entry.append(result["_source"][field])
                results.append(",".join(entry))
            print("{} Results".format(hit_count))
            print("{}".format(json.dumps(results, indent=4)))

    def print_results(self):
        print("{}".format(json.dumps(self.results[0], indent=4)))


class QueryBuildException(Exception):
    pass
