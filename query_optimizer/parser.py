from __future__ import annotations
from typing import Optional
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.query_tree import QueryTree
from query_optimizer.query_token import Token, TokenType


class ParserError(Exception):
    def __init__(self, message: str, token: Optional[Token] = None):
        if token:
            super().__init__(
                f"Parser Error at line {token.line}, column {token.column}: {message}\n"
                f"Token: {token.type.name} = '{token.value}'"
            )
        else:
            super().__init__(f"Parser Error: {message}")


class Parser:
    def __init__(self, tokenizer: Tokenizer):
        self.tokenizer = tokenizer
        self.current_token: Optional[Token] = None
        self.peek_token: Optional[Token] = None

        self.advance()
        self.advance()

    def advance(self) -> None:
        self.current_token = self.peek_token
        self.peek_token = self.tokenizer.get_next_token()

    def expect(self, token_type: TokenType) -> Token:
        if self.current_token is None or self.current_token.type != token_type:
            expected = token_type.name
            actual = self.current_token.type.name if self.current_token else "EOF"
            raise ParserError(
                f"Expected {expected} but got {actual}",
                self.current_token
            )

        token = self.current_token
        self.advance()
        return token

    def match(self, *token_types: TokenType) -> bool:
        if self.current_token is None:
            return False
        return self.current_token.type in token_types

    def match_value(self, *values: str) -> bool:
        if self.current_token is None:
            return False
        return self.current_token.value.upper() in [v.upper() for v in values]

    def consume_if(self, token_type: TokenType) -> bool:
        if self.match(token_type):
            self.advance()
            return True
        return False


    def parse(self) -> QueryTree:
        return self.parse_statement()

    def parse_statement(self) -> QueryTree:
        if self.current_token is None:
            raise ParserError("Unexpected end of input")

        if self.match(TokenType.KEYWORD_SELECT):
            return self.parse_select()
        elif self.match(TokenType.KEYWORD_UPDATE):
            return self.parse_update()
        elif self.match(TokenType.KEYWORD_INSERT):
            return self.parse_insert()
        elif self.match(TokenType.KEYWORD_DELETE):
            return self.parse_delete()
        elif self.match(TokenType.KEYWORD_BEGIN_TRANSACTION):
            return self.parse_transaction()
        else:
            raise ParserError(
                f"Expected statement keyword (SELECT, UPDATE, INSERT, DELETE, BEGIN TRANSACTION)",
                self.current_token
            )

    # Select Query

    def parse_select(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_SELECT)

        columns = self.parse_columns()
        project_node = QueryTree("PROJECT", columns)

        self.expect(TokenType.KEYWORD_FROM)
        source = self.parse_from()

        if self.match(TokenType.KEYWORD_WHERE):
            source = self.parse_where(source)

        if self.match(TokenType.KEYWORD_ORDER_BY):
            source = self.parse_order(source)

        if self.match(TokenType.KEYWORD_LIMIT):
            self.advance()
            if self.match(TokenType.LITERAL_NUMBER):
                self.advance()

        project_node.add_child(source)
        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        return project_node

    def parse_columns(self) -> str:
        columns = []

        if self.match(TokenType.OPERATOR_MULTIPLY):
            self.advance()
            return "*"

        if not self.match(TokenType.IDENTIFIER, TokenType.LITERAL_NUMBER):
            raise ParserError("Expected column name", self.current_token)

        columns.append(self.current_token.value)
        self.advance()

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()

            if self.match(TokenType.IDENTIFIER, TokenType.LITERAL_NUMBER):
                columns.append(self.current_token.value)
                self.advance()
            else:
                raise ParserError("Expected column name after comma", self.current_token)

        return ", ".join(columns)

    def parse_from(self) -> QueryTree:
        left = self.parse_table()

        while self.match(TokenType.KEYWORD_JOIN) or (
            self.match(TokenType.KEYWORD_NATURAL) and
            self.peek_token and self.peek_token.type == TokenType.KEYWORD_JOIN
        ):
            left = self.parse_join(left)

        return left

    def parse_table(self) -> QueryTree:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name", self.current_token)

        table_name = self.current_token.value
        self.advance()

        return QueryTree("RELATION", table_name)

    def parse_join(self, left_table: QueryTree) -> QueryTree:
        is_natural = self.consume_if(TokenType.KEYWORD_NATURAL)

        self.expect(TokenType.KEYWORD_JOIN)

        right_table = self.parse_table()

        if is_natural:
            join_value = "NATURAL"
        elif self.match(TokenType.KEYWORD_ON):
            self.advance()
            condition = self.parse_condition()
            join_value = f"ON {condition}"
        else:
            raise ParserError("JOIN must have either NATURAL or ON clause", self.current_token)

        join_node = QueryTree("JOIN", join_value)
        join_node.add_child(left_table)
        join_node.add_child(right_table)

        return join_node

    def parse_where(self, source: QueryTree) -> QueryTree:
        self.expect(TokenType.KEYWORD_WHERE)

        condition_tree = self.parse_boolean_expr()

        if condition_tree.type == "OPERATOR":
            if condition_tree.val in {"AND", "OR"}:
                operator_node = QueryTree("OPERATOR_S", condition_tree.val)
                operator_node.add_child(source)
                for child in condition_tree.childs:
                    operator_node.add_child(child)
                return operator_node
            if condition_tree.val == "NOT":
                operator_node = QueryTree("OPERATOR_S", "AND")
                operator_node.add_child(source)
                operator_node.add_child(condition_tree)
                operator_node.add_child(QueryTree("FILTER", "WHERE TRUE"))
                return operator_node

        if condition_tree.type == "FILTER":
            self._attach_source(condition_tree, source)
            return condition_tree

        rendered = self._condition_to_string(condition_tree)
        fallback_filter = QueryTree("FILTER", f"WHERE {rendered}")
        fallback_filter.add_child(source)
        return fallback_filter

    def parse_in_condition(self) -> QueryTree:
        column_name = self.current_token.value
        self.advance()

        if not self.match_value("IN"):
            raise ParserError("Expected IN keyword", self.current_token)
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        array_values = self.parse_array()
        self.expect(TokenType.DELIMITER_RPAREN)

        filter_node = QueryTree("FILTER", f"IN {column_name}")
        array_node = QueryTree("ARRAY", f"({array_values})")
        filter_node.add_child(array_node)

        return filter_node

    def parse_exists_condition(self) -> QueryTree:
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        subquery = self.parse_subquery()
        self.expect(TokenType.DELIMITER_RPAREN)

        filter_node = QueryTree("FILTER", "EXIST")
        filter_node.add_child(subquery)

        return filter_node

    def parse_subquery(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_SELECT)

        columns = self.parse_columns()
        project_node = QueryTree("PROJECT", columns)

        self.expect(TokenType.KEYWORD_FROM)
        source = self.parse_from()

        if self.match(TokenType.KEYWORD_WHERE):
            source = self.parse_where(source)

        if self.match(TokenType.KEYWORD_ORDER_BY):
            source = self.parse_order(source)

        project_node.add_child(source)

        return project_node

    def parse_array(self) -> str:
        values = []

        if self.match(TokenType.LITERAL_NUMBER, TokenType.LITERAL_STRING, TokenType.IDENTIFIER):
            val = self.current_token.value
            if self.match(TokenType.LITERAL_STRING):
                val = f"'{val}'"
            values.append(val)
            self.advance()
        else:
            raise ParserError("Expected value in array", self.current_token)

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()

            if self.match(TokenType.LITERAL_NUMBER, TokenType.LITERAL_STRING, TokenType.IDENTIFIER):
                val = self.current_token.value
                if self.match(TokenType.LITERAL_STRING):
                    val = f"'{val}'"
                values.append(val)
                self.advance()
            else:
                raise ParserError("Expected value after comma", self.current_token)

        return ", ".join(values)

    def parse_order(self, source: QueryTree) -> QueryTree:
        self.expect(TokenType.KEYWORD_ORDER_BY)

        columns = self.parse_columns()

        sort_node = QueryTree("SORT", columns)
        sort_node.add_child(source)

        return sort_node

    # Update Query

    def parse_update(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_UPDATE)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after UPDATE", self.current_token)

        table_name = self.current_token.value
        self.advance()

        self.expect(TokenType.KEYWORD_SET)
        set_expressions = self.parse_set()

        update_node = QueryTree("UPDATE", set_expressions)
        relation_node = QueryTree("RELATION", table_name)

        if self.match(TokenType.KEYWORD_WHERE):
            filter_node = self.parse_where(relation_node)
            update_node.add_child(filter_node)
        else:
            update_node.add_child(relation_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        return update_node

    def parse_set(self) -> str:
        assignments = []

        assignments.append(self.parse_assignment())

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            assignments.append(self.parse_assignment())

        return ", ".join(assignments)

    def parse_assignment(self) -> str:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected column name", self.current_token)

        column = self.current_token.value
        self.advance()

        self.expect(TokenType.OPERATOR_EQUAL)

        value_token = self.current_token
        if not self.match(TokenType.LITERAL_STRING, TokenType.LITERAL_NUMBER, TokenType.LITERAL_BOOLEAN,
                          TokenType.LITERAL_NULL, TokenType.IDENTIFIER):
            raise ParserError("Expected value in assignment", self.current_token)

        value = value_token.value

        if value_token.type == TokenType.LITERAL_STRING:
            value = f"'{value}'"

        self.advance()

        return f"{column} = {value}"

    # Insert Query

    def parse_insert(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_INSERT)
        self.expect(TokenType.KEYWORD_INTO)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after INTO", self.current_token)

        table_name = self.current_token.value
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        columns = []

        if self.match(TokenType.IDENTIFIER):
            columns.append(self.current_token.value)
            self.advance()
        else:
            raise ParserError("Expected column name", self.current_token)

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            if self.match(TokenType.IDENTIFIER):
                columns.append(self.current_token.value)
                self.advance()
            else:
                raise ParserError("Expected column name after comma", self.current_token)

        self.expect(TokenType.DELIMITER_RPAREN)

        if not self.match(TokenType.IDENTIFIER) or self.current_token.value.upper() != "VALUES":
            raise ParserError("Expected VALUES keyword", self.current_token)
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        values = []

        value_token = self.current_token
        if self.match(TokenType.LITERAL_STRING, TokenType.LITERAL_NUMBER, TokenType.LITERAL_BOOLEAN,
                      TokenType.LITERAL_NULL, TokenType.IDENTIFIER):
            val = value_token.value
            if value_token.type == TokenType.LITERAL_STRING:
                val = f"'{val}'"
            values.append(val)
            self.advance()
        else:
            raise ParserError("Expected value", self.current_token)

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            value_token = self.current_token
            if self.match(TokenType.LITERAL_STRING, TokenType.LITERAL_NUMBER, TokenType.LITERAL_BOOLEAN,
                          TokenType.LITERAL_NULL, TokenType.IDENTIFIER):
                val = value_token.value
                if value_token.type == TokenType.LITERAL_STRING:
                    val = f"'{val}'"
                values.append(val)
                self.advance()
            else:
                raise ParserError("Expected value after comma", self.current_token)

        self.expect(TokenType.DELIMITER_RPAREN)

        if len(columns) != len(values):
            raise ParserError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")

        assignments = [f"{col} = {val}" for col, val in zip(columns, values)]
        insert_value = ", ".join(assignments)

        insert_node = QueryTree("INSERT", insert_value)
        relation_node = QueryTree("RELATION", table_name)
        insert_node.add_child(relation_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        return insert_node

    # Delete

    def parse_delete(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_DELETE)
        self.expect(TokenType.KEYWORD_FROM)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after FROM", self.current_token)

        table_name = self.current_token.value
        self.advance()

        delete_node = QueryTree("DELETE", "")
        relation_node = QueryTree("RELATION", table_name)

        if self.match(TokenType.KEYWORD_WHERE):
            filter_node = self.parse_where(relation_node)
            delete_node.add_child(filter_node)
        else:
            delete_node.add_child(relation_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        return delete_node

    # Transaction Parse

    def parse_transaction(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_BEGIN_TRANSACTION)
        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        transaction_node = QueryTree("BEGIN_TRANSACTION", "")

        while not self.match(TokenType.KEYWORD_COMMIT):
            if self.current_token is None or self.match(TokenType.EOF):
                raise ParserError("Expected COMMIT to close transaction")

            statement = self.parse_statement()
            transaction_node.add_child(statement)

        self.expect(TokenType.KEYWORD_COMMIT)
        commit_node = QueryTree("COMMIT", "")
        transaction_node.add_child(commit_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)

        return transaction_node

    # Helper Method

    def parse_boolean_expr(self) -> QueryTree:
        return self.parse_or_expr()

    def parse_or_expr(self) -> QueryTree:
        left = self.parse_and_expr()
        children = [left]

        while self.match(TokenType.KEYWORD_OR):
            self.advance()
            children.append(self.parse_and_expr())

        if len(children) == 1:
            return left

        return self._build_flat_operator("OR", children)

    def parse_and_expr(self) -> QueryTree:
        left = self.parse_not_expr()
        children = [left]

        while self.match(TokenType.KEYWORD_AND):
            self.advance()
            children.append(self.parse_not_expr())

        if len(children) == 1:
            return left

        return self._build_flat_operator("AND", children)

    def parse_not_expr(self) -> QueryTree:
        if self.match(TokenType.KEYWORD_NOT):
            self.advance()
            operand = self.parse_not_expr()
            node = QueryTree("OPERATOR", "NOT")
            node.add_child(operand)
            return node
        return self.parse_primary_condition()

    def parse_primary_condition(self) -> QueryTree:
        if self.match(TokenType.DELIMITER_LPAREN):
            self.advance()
            expr = self.parse_boolean_expr()
            self.expect(TokenType.DELIMITER_RPAREN)
            return expr

        if self.match(TokenType.IDENTIFIER) and self.match_value("EXISTS"):
            return self.parse_exists_condition()

        if self.match(TokenType.IDENTIFIER):
            if self.peek_token and self.peek_token.type == TokenType.IDENTIFIER and self.peek_token.value.upper() == "IN":
                return self.parse_in_condition()

        condition_text = self._collect_condition_tokens()
        filter_node = QueryTree("FILTER", f"WHERE {condition_text}")
        return filter_node

    def _collect_condition_tokens(self) -> str:
        tokens = []
        paren_depth = 0
        terminating = {
            TokenType.KEYWORD_ORDER_BY, TokenType.KEYWORD_LIMIT, TokenType.DELIMITER_SEMICOLON,
            TokenType.KEYWORD_JOIN, TokenType.KEYWORD_NATURAL,
            TokenType.EOF
        }
        logical_tokens = {TokenType.KEYWORD_AND, TokenType.KEYWORD_OR}

        while self.current_token:
            if paren_depth == 0 and (self.current_token.type in terminating or self.current_token.type in logical_tokens):
                break

            if self.current_token.type == TokenType.DELIMITER_RPAREN and paren_depth == 0:
                break

            if self.current_token.type == TokenType.DELIMITER_LPAREN:
                paren_depth += 1
                tokens.append(self.current_token.value)
                self.advance()
                continue

            if self.current_token.type == TokenType.DELIMITER_RPAREN:
                paren_depth -= 1
                tokens.append(self.current_token.value)
                self.advance()
                continue

            if self.match(TokenType.IDENTIFIER) and self.current_token.value.upper() == "IN":
                tokens.append("IN")
                self.advance()
                self.expect(TokenType.DELIMITER_LPAREN)
                array_values = self.parse_array()
                self.expect(TokenType.DELIMITER_RPAREN)
                tokens.append(f"({array_values})")
                continue

            if self.match(TokenType.LITERAL_STRING):
                tokens.append(f"'{self.current_token.value}'")
            else:
                tokens.append(self.current_token.value)

            self.advance()

        if not tokens:
            raise ParserError("Expected condition expression", self.current_token)

        return " ".join(tokens)

    def _build_flat_operator(self, operator: str, children: list[QueryTree]) -> QueryTree:
        node = QueryTree("OPERATOR", operator)
        for child in children:
            node.add_child(child)
        return node

    def _attach_source(self, filter_node: QueryTree, source: QueryTree) -> None:
        if filter_node.childs:
            filter_node.childs.insert(0, source)
            source.parent = filter_node
        else:
            filter_node.add_child(source)

    def _condition_to_string(self, node: QueryTree) -> str:
        if node.type == "FILTER":
            parts = node.val.split(maxsplit=1)
            if len(parts) == 2:
                base = parts[1]
            else:
                base = parts[0]
            if node.childs and node.childs[0].type == "ARRAY":
                return f"{base} {node.childs[0].val}"
            return base

        if node.type == "OPERATOR":
            if node.val == "NOT":
                return f"NOT ({self._condition_to_string(node.childs[0])})"
            joined = f" {node.val} ".join(self._condition_to_string(child) for child in node.childs)
            return f"({joined})"

        return node.val

    def parse_condition(self) -> str:
        condition_tokens = []

        terminating_keywords = {
            TokenType.KEYWORD_ORDER_BY, TokenType.KEYWORD_LIMIT, TokenType.DELIMITER_SEMICOLON,
            TokenType.KEYWORD_JOIN, TokenType.KEYWORD_NATURAL,
            TokenType.DELIMITER_RPAREN,
            TokenType.EOF
        }

        while self.current_token and self.current_token.type not in terminating_keywords:
            if self.match(TokenType.KEYWORD_WHERE, TokenType.KEYWORD_SELECT, TokenType.KEYWORD_FROM):
                break

            condition_tokens.append(self.current_token.value)
            self.advance()

        if not condition_tokens:
            raise ParserError("Expected condition expression", self.current_token)

        return " ".join(condition_tokens)
