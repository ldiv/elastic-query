import re
import json


OPERATORS = ['or', 'and']


def is_open_paren(token):
    return token == "("


def is_closed_paren(token):
    return token == ")"


def is_operator(token):
    return token in OPERATORS or token.lower() in OPERATORS


def is_operand(token):
    if is_closed_paren(token) or is_open_paren(token) or is_operator(token):
        return False
    return True


def has_lower_precedence(o1, o2):
    if is_open_paren(o1):
        return True
    return OPERATORS.index(o1) < OPERATORS.index(o2)


def evaluate(expression):
    return expression


def read_next_token(expression, token_patterns):
    operators = OPERATORS + [o.upper() for o in OPERATORS]
    token_patterns = token_patterns + [" {} ".format(op) for op in operators] + ['\(', '\)']
    for token_format in token_patterns:
        m = re.match(token_format, expression)
        if m:
            expression = expression[len(m.group()):]
            return m.group().strip(), expression
    return None, expression


def parse_into_ast(expression, token_patterns):
    operator = []
    operand = []
    token, expression = read_next_token(expression, token_patterns)
    if not token:
        return None
    while token:
        if is_operand(token):
            operand.append(token)
        elif is_open_paren(token):
            operator.append(token)
        elif is_operator(token):
            current_operator = token
            if operator:
                pass
            while operator \
                    and not has_lower_precedence(operator[-1], token) \
                    and not is_open_paren(operator[-1]):
                term2 = operand.pop()
                term1 = operand.pop()
                token = operator.pop()
                operand.append(evaluate({token: [term1, term2]}))
            operator.append(current_operator)
        elif is_closed_paren(token):
            while operator and not is_open_paren(operator[-1]):
                term2 = operand.pop()
                term1 = operand.pop()
                op = operator.pop()
                operand.append(evaluate({op: [term1, term2]}))
            operator.pop()  # Removes open paren
        if not expression:
            break
        token, expression = read_next_token(expression, token_patterns)

    # Evaluate what remains
    while operator:
        term2 = operand.pop()
        term1 = operand.pop()
        token = operator.pop()
        operand.append(evaluate({token: [term1, term2]}))

    return json.loads(json.dumps(operand.pop()))
