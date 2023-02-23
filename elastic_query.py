import re
import json
import ast


class ElasticQuery:

    def __init__(self, size=None, fields=None):
        self.query = None
        self.query_size = size or 10
        self.fields = fields or []

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
        if ast_op.lower() == "and":
            if query_op == "~=":
                return "must"
            elif query_op == "!~":
                return "must_not"
            return "must"
        if ast_op.lower() == "or":
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

        resolved_query_ast = self.resolve_ast(query_ast) if query_ast else None
        return resolved_query_ast

    def _parse_query_expression(self, query_expression):
        """
        Parses the expression into search and agg parts

        :param query_expression: Raw query expression from client code
        :return: two element tuple containing the parsed search and agg queries
        """
        search_expression, *agg_expression = re.split("\|", query_expression)
        # If the search expression is just a keyword build a querystring query
        keyword_pattern = "^[\w_-]+\s*$"
        keyword_match = re.match(keyword_pattern, search_expression)
        if keyword_match:
            return self._build_querystring_query(search_expression), {}

        search_query_parsed = self._parse_search_query_terms(query_expression)
        agg_query_parsed = None  # TODO: placeholder for call to process aggregations

        if search_query_parsed:
            return search_query_parsed, agg_query_parsed
        raise QueryBuildException("Invalid Query: {}".format(query_expression))

    def build_query(self, query_expression):
        try:
            search_query, agg_query = self._parse_query_expression(query_expression)
        except QueryBuildException:
            # TODO: log
            return None
        self.query = {"query": search_query, "size": self.query_size}
        if self.fields:
            self.query["_source"] = self.fields
        if agg_query:
            self.query["aggregations"] = agg_query
        return self.query

    def print_query(self):
        print(json.dumps(self.query, indent=2))


class QueryBuildException(Exception):
    pass
