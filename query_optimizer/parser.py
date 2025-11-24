from __future__ import annotations
from typing import Optional, List
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

    def expect(self, *token_types: TokenType) -> Token:
        if self.current_token is None or self.current_token.type not in token_types:
            expected = "/".join(t.name for t in token_types)
            actual = self.current_token.type.name if self.current_token else "EOF"
            raise ParserError(f"Expected {expected} but got {actual}", self.current_token)
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

    def consume_if(self, *token_types: TokenType) -> bool:
        if self.match(*token_types):
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
        if self.match(TokenType.KEYWORD_UPDATE):
            return self.parse_update()
        if self.match(TokenType.KEYWORD_INSERT):
            return self.parse_insert()
        if self.match(TokenType.KEYWORD_DELETE):
            return self.parse_delete()
        if self.match(TokenType.KEYWORD_BEGIN_TRANSACTION):
            return self.parse_transaction()
        if self.match(TokenType.KEYWORD_CREATE):
            return self.parse_create_table()
        if self.match(TokenType.KEYWORD_DROP):
            return self.parse_drop_table()

        raise ParserError(
            "Expected statement keyword (SELECT, UPDATE, INSERT, DELETE, BEGIN TRANSACTION)",
            self.current_token
        )

    def parse_select(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_SELECT)

        is_wildcard, columns = self.parse_select_list()
        project_node = QueryTree("PROJECT", "*" if is_wildcard else "")
        if not is_wildcard:
            for col in columns:
                project_node.add_child(col)

        self.expect(TokenType.KEYWORD_FROM)
        source = self.parse_from()

        if self.match(TokenType.KEYWORD_WHERE):
            source = self.parse_where(source)

        if self.match(TokenType.KEYWORD_ORDER_BY):
            source = self.parse_order(source)

        if self.match(TokenType.KEYWORD_LIMIT):
            source = self.parse_limit(source)

        project_node.add_child(source)
        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return project_node

    def parse_select_list(self) -> tuple[bool, List[QueryTree]]:
        if self.match(TokenType.OPERATOR_MULTIPLY):
            self.advance()
            return True, []

        items = [self.parse_value_expr()]
        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            items.append(self.parse_value_expr())
        return False, items

    def parse_from(self) -> QueryTree:
        left = self.parse_table_ref()
        while self.match(TokenType.KEYWORD_JOIN, TokenType.KEYWORD_INNER) or (
            self.match(TokenType.KEYWORD_NATURAL) and
            self.peek_token and self.peek_token.type == TokenType.KEYWORD_JOIN
        ):
            left = self.parse_join(left)
        return left

    def parse_table_ref(self) -> QueryTree:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name", self.current_token)
        table_name_token = self.current_token
        self.advance()
        table_node = QueryTree("RELATION", table_name_token.value)

        if self.match(TokenType.KEYWORD_AS) or (self.match(TokenType.IDENTIFIER) and self.match_value("AS")):
            self.advance()
        if self.match(TokenType.IDENTIFIER):
            alias_node = QueryTree("ALIAS", self.current_token.value)
            alias_node.add_child(table_node)
            self.advance()
            return alias_node
        return table_node

    def parse_join(self, left_table: QueryTree) -> QueryTree:
        join_type = "INNER"
        if self.match(TokenType.KEYWORD_NATURAL):
            join_type = "NATURAL"
            self.advance()
        elif self.match(TokenType.KEYWORD_INNER):
            join_type = "INNER"
            self.advance()

        self.expect(TokenType.KEYWORD_JOIN)
        right_table = self.parse_table_ref()

        join_node = QueryTree("JOIN", join_type)
        join_node.add_child(left_table)
        join_node.add_child(right_table)

        if join_type == "NATURAL":
            return join_node

        if self.match(TokenType.KEYWORD_ON):
            self.advance()
            condition = self.parse_boolean_expr()
            join_node.add_child(condition)
            return join_node

        raise ParserError("JOIN must have NATURAL or ON clause", self.current_token)

    def parse_where(self, source: QueryTree) -> QueryTree:
        self.expect(TokenType.KEYWORD_WHERE)
        condition = self.parse_boolean_expr()

        filter_node = QueryTree("FILTER", "")
        filter_node.add_child(source)
        filter_node.add_child(condition)
        return filter_node

    def parse_order(self, source: QueryTree) -> QueryTree:
        self.expect(TokenType.KEYWORD_ORDER_BY)
        order_expr = self.parse_value_expr()

        direction = "ASC"
        if self.match_value("ASC", "DESC"):
            direction = self.current_token.value.upper()
            self.advance()

        sort_node = QueryTree("SORT", direction)
        sort_node.add_child(order_expr)
        sort_node.add_child(source)
        return sort_node

    def parse_limit(self, source: QueryTree) -> QueryTree:
        self.expect(TokenType.KEYWORD_LIMIT)
        if not self.match(TokenType.LITERAL_NUMBER):
            raise ParserError("Expected numeric literal after LIMIT", self.current_token)
        limit_value = self.current_token.value
        self.advance()

        limit_node = QueryTree("LIMIT", limit_value)
        limit_node.add_child(source)
        return limit_node

    def parse_update(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_UPDATE)
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after UPDATE", self.current_token)

        table_name = self.current_token.value
        self.advance()
        relation = QueryTree("RELATION", table_name)

        self.expect(TokenType.KEYWORD_SET)
        assignments = self.parse_assignments()

        update_node = QueryTree("UPDATE_QUERY", "")
        update_node.add_child(relation)
        for assign in assignments:
            update_node.add_child(assign)

        if self.match(TokenType.KEYWORD_WHERE):
            filter_node = self.parse_where(relation.clone(deep=True))
            update_node.add_child(filter_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return update_node

    def parse_assignments(self) -> List[QueryTree]:
        assignments = [self.parse_assignment()]
        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            assignments.append(self.parse_assignment())
        return assignments

    def parse_assignment(self) -> QueryTree:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected column name in assignment", self.current_token)

        column_ref = self.build_column_ref(self.current_token.value)
        self.advance()
        self.expect(TokenType.OPERATOR_EQUAL)

        value_expr = self.parse_value_expr()
        assignment_node = QueryTree("ASSIGNMENT", "")
        assignment_node.add_child(column_ref)
        assignment_node.add_child(value_expr)
        return assignment_node

    def parse_insert(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_INSERT)
        self.expect(TokenType.KEYWORD_INTO)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after INTO", self.current_token)
        table_name = self.current_token.value
        self.advance()
        relation = QueryTree("RELATION", table_name)

        self.expect(TokenType.DELIMITER_LPAREN)
        columns = self.parse_identifier_list()
        self.expect(TokenType.DELIMITER_RPAREN)

        if not (self.match(TokenType.IDENTIFIER) and self.current_token.value.upper() == "VALUES"):
            raise ParserError("Expected VALUES keyword", self.current_token)
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        values = self.parse_value_list()
        self.expect(TokenType.DELIMITER_RPAREN)

        if len(columns) != len(values):
            raise ParserError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")

        insert_node = QueryTree("INSERT_QUERY", "")
        insert_node.add_child(relation)

        column_list_node = QueryTree("COLUMN_LIST", "")
        for col in columns:
            column_list_node.add_child(QueryTree("IDENTIFIER", col))
        insert_node.add_child(column_list_node)

        values_node = QueryTree("VALUES_CLAUSE", "")
        for val in values:
            values_node.add_child(val)
        insert_node.add_child(values_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return insert_node

    def parse_delete(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_DELETE)
        self.expect(TokenType.KEYWORD_FROM)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after FROM", self.current_token)
        table_name = self.current_token.value
        self.advance()
        relation = QueryTree("RELATION", table_name)

        delete_node = QueryTree("DELETE_QUERY", "")
        delete_node.add_child(relation)

        if self.match(TokenType.KEYWORD_WHERE):
            filter_node = self.parse_where(relation.clone(deep=True))
            delete_node.add_child(filter_node)

        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return delete_node

    def parse_create_table(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_CREATE)
        self.expect(TokenType.KEYWORD_TABLE)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after CREATE TABLE", self.current_token)
        table_name = self.current_token.value
        self.advance()

        self.expect(TokenType.DELIMITER_LPAREN)
        column_defs = [self.parse_column_def()]
        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            column_defs.append(self.parse_column_def())
        self.expect(TokenType.DELIMITER_RPAREN)

        create_node = QueryTree("CREATE_TABLE", "")
        create_node.add_child(QueryTree("IDENTIFIER", table_name))
        col_list = QueryTree("COLUMN_DEF_LIST", "")
        for col_def in column_defs:
            col_list.add_child(col_def)
        create_node.add_child(col_list)
        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return create_node

    def parse_column_def(self) -> QueryTree:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected column name", self.current_token)
        col_name = self.current_token.value
        self.advance()

        data_type = self.parse_data_type()
        constraint = self.parse_column_constraint()

        col_def = QueryTree("COLUMN_DEF", "")
        col_def.add_child(QueryTree("IDENTIFIER", col_name))
        col_def.add_child(data_type)
        if constraint:
            col_def.add_child(constraint)
        return col_def

    def parse_data_type(self) -> QueryTree:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected data type", self.current_token)
        type_name = self.current_token.value.upper()
        self.advance()
        if type_name in {"CHAR", "VARCHAR"} and self.match(TokenType.DELIMITER_LPAREN):
            self.advance()
            if not self.match(TokenType.LITERAL_NUMBER):
                raise ParserError("Expected length for data type", self.current_token)
            length = self.current_token.value
            self.advance()
            self.expect(TokenType.DELIMITER_RPAREN)
            return QueryTree("DATA_TYPE", f"{type_name}({length})")
        return QueryTree("DATA_TYPE", type_name)

    def parse_column_constraint(self) -> Optional[QueryTree]:
        if self.match(TokenType.KEYWORD_PRIMARY) or (self.match(TokenType.IDENTIFIER) and self.match_value("PRIMARY")):
            self.advance()
            if self.match(TokenType.KEYWORD_KEY) or (self.match(TokenType.IDENTIFIER) and self.match_value("KEY")):
                self.advance()
            return QueryTree("PRIMARY_KEY", "")

        if self.match(TokenType.KEYWORD_FOREIGN) or (self.match(TokenType.IDENTIFIER) and self.match_value("FOREIGN")):
            self.advance()
            if self.match(TokenType.KEYWORD_KEY) or (self.match(TokenType.IDENTIFIER) and self.match_value("KEY")):
                self.advance()
            if not (self.match(TokenType.KEYWORD_REFERENCES) or (self.match(TokenType.IDENTIFIER) and self.match_value("REFERENCES"))):
                raise ParserError("Expected REFERENCES in FOREIGN KEY", self.current_token)
            self.advance()
            if not self.match(TokenType.IDENTIFIER):
                raise ParserError("Expected referenced table name", self.current_token)
            ref_table = self.current_token.value
            self.advance()
            self.expect(TokenType.DELIMITER_LPAREN)
            if not self.match(TokenType.IDENTIFIER):
                raise ParserError("Expected referenced column name", self.current_token)
            ref_col = self.current_token.value
            self.advance()
            self.expect(TokenType.DELIMITER_RPAREN)

            references = QueryTree("REFERENCES", ref_table)
            references.add_child(QueryTree("IDENTIFIER", ref_col))
            fk = QueryTree("FOREIGN_KEY", "")
            fk.add_child(references)
            return fk
        return None

    def parse_drop_table(self) -> QueryTree:
        self.expect(TokenType.KEYWORD_DROP)
        self.expect(TokenType.KEYWORD_TABLE)

        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected table name after DROP TABLE", self.current_token)
        table_name = self.current_token.value
        self.advance()

        behavior = ""
        if self.match(TokenType.KEYWORD_CASCADE) or (self.match(TokenType.IDENTIFIER) and self.match_value("CASCADE")):
            behavior = "CASCADE"
            self.advance()
        elif self.match(TokenType.KEYWORD_RESTRICT) or (self.match(TokenType.IDENTIFIER) and self.match_value("RESTRICT")):
            behavior = "RESTRICT"
            self.advance()

        drop_node = QueryTree("DROP_TABLE", behavior)
        drop_node.add_child(QueryTree("IDENTIFIER", table_name))
        self.consume_if(TokenType.DELIMITER_SEMICOLON)
        return drop_node

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
        return self._build_operator("OR", children)

    def parse_and_expr(self) -> QueryTree:
        left = self.parse_not_expr()
        children = [left]

        while self.match(TokenType.KEYWORD_AND):
            self.advance()
            children.append(self.parse_not_expr())

        if len(children) == 1:
            return left
        return self._build_operator("AND", children)

    def parse_not_expr(self) -> QueryTree:
        if self.match(TokenType.KEYWORD_NOT):
            if self.peek_token and (
                self.peek_token.type == TokenType.KEYWORD_EXISTS
                or (self.peek_token.type == TokenType.IDENTIFIER and self.peek_token.value.upper() == "EXISTS")
            ):
                self.advance()  # NOT
                self.advance()  # EXISTS
                self.expect(TokenType.DELIMITER_LPAREN)
                subquery = self.parse_subquery()
                self.expect(TokenType.DELIMITER_RPAREN)
                node = QueryTree("NOT_EXISTS_EXPR", "")
                node.add_child(subquery)
                return node
            self.advance()
            operand = self.parse_not_expr()
            node = QueryTree("OPERATOR", "NOT")
            node.add_child(operand)
            return node
        return self.parse_condition_atom()

    def parse_condition_atom(self) -> QueryTree:
        if self.match(TokenType.DELIMITER_LPAREN):
            self.advance()
            expr = self.parse_boolean_expr()
            self.expect(TokenType.DELIMITER_RPAREN)
            return expr

        # EXISTS
        if (self.match(TokenType.KEYWORD_EXISTS) or (self.match(TokenType.IDENTIFIER) and self.match_value("EXISTS"))) or (
            self.match(TokenType.KEYWORD_NOT) and self.peek_token and (
                self.peek_token.type == TokenType.KEYWORD_EXISTS or self.peek_token.value.upper() == "EXISTS")
        ):
            negated = False
            if self.match(TokenType.KEYWORD_NOT):
                negated = True
                self.advance()
            if self.match(TokenType.KEYWORD_EXISTS) or (self.match(TokenType.IDENTIFIER) and self.match_value("EXISTS")):
                self.advance()
            self.expect(TokenType.DELIMITER_LPAREN)
            subquery = self.parse_subquery()
            self.expect(TokenType.DELIMITER_RPAREN)
            exists_node = QueryTree("NOT_EXISTS_EXPR" if negated else "EXISTS_EXPR", "")
            exists_node.add_child(subquery)
            return exists_node

        # Standard comparison
        left_expr = self.parse_value_expr()

        # NOT IN
        if self.match(TokenType.KEYWORD_NOT) and self.peek_token and self.peek_token.type == TokenType.KEYWORD_IN:
            self.advance()
            self.advance()
            list_node = self.parse_in_list_or_subquery()
            in_node = QueryTree("NOT_IN_EXPR", "")
            in_node.add_child(left_expr)
            in_node.add_child(list_node)
            return in_node

        # IN
        if self.match(TokenType.KEYWORD_IN):
            self.advance()
            list_node = self.parse_in_list_or_subquery()
            in_node = QueryTree("IN_EXPR", "")
            in_node.add_child(left_expr)
            in_node.add_child(list_node)
            return in_node

        # BETWEEN / NOT BETWEEN
        if (self.match(TokenType.KEYWORD_NOT) and self.peek_token and (
            self.peek_token.type == TokenType.KEYWORD_BETWEEN or self.peek_token.value.upper() == "BETWEEN")) or (
            self.match(TokenType.KEYWORD_BETWEEN) or (self.match(TokenType.IDENTIFIER) and self.match_value("BETWEEN"))
        ):
            negated = False
            if self.match(TokenType.KEYWORD_NOT):
                negated = True
                self.advance()
            if self.match(TokenType.KEYWORD_BETWEEN) or (self.match(TokenType.IDENTIFIER) and self.match_value("BETWEEN")):
                self.advance()
            low = self.parse_value_expr()
            if not (self.match(TokenType.KEYWORD_AND) or (self.match(TokenType.IDENTIFIER) and self.match_value("AND"))):
                raise ParserError("Expected AND in BETWEEN clause", self.current_token)
            self.advance()
            high = self.parse_value_expr()
            between_node = QueryTree("NOT_BETWEEN_EXPR" if negated else "BETWEEN_EXPR", "")
            between_node.add_child(left_expr)
            between_node.add_child(low)
            between_node.add_child(high)
            return between_node

        # IS [NOT] NULL
        if (self.match(TokenType.KEYWORD_IS) or (self.match(TokenType.IDENTIFIER) and self.match_value("IS"))):
            self.advance()
            negated = False
            if self.match(TokenType.KEYWORD_NOT):
                negated = True
                self.advance()
            if not (self.match(TokenType.LITERAL_NULL) or (self.match(TokenType.IDENTIFIER) and self.match_value("NULL"))):
                raise ParserError("Expected NULL after IS", self.current_token)
            self.advance()
            node = QueryTree("IS_NOT_NULL_EXPR" if negated else "IS_NULL_EXPR", "")
            node.add_child(left_expr)
            return node

        # LIKE / NOT LIKE
        if self.match(TokenType.KEYWORD_NOT) and self.peek_token and (
            self.peek_token.type == TokenType.KEYWORD_LIKE or self.peek_token.value.upper() == "LIKE"):
            self.advance()
            self.advance()
            pattern = self.parse_value_expr()
            like_node = QueryTree("NOT_LIKE_EXPR", "")
            like_node.add_child(left_expr)
            like_node.add_child(pattern)
            return like_node
        if self.match(TokenType.KEYWORD_LIKE) or (self.match(TokenType.IDENTIFIER) and self.match_value("LIKE")):
            self.advance()
            pattern = self.parse_value_expr()
            like_node = QueryTree("LIKE_EXPR", "")
            like_node.add_child(left_expr)
            like_node.add_child(pattern)
            return like_node

        # Comparison operators
        if self.match(
            TokenType.OPERATOR_EQUAL,
            TokenType.OPERATOR_NOT_EQUAL,
            TokenType.OPERATOR_GREATER_EQUAL,
            TokenType.OPERATOR_LESS_EQUAL,
            TokenType.OPERATOR_GREATER_THAN,
            TokenType.OPERATOR_LESS_THAN
        ):
            op_token = self.current_token
            self.advance()
            right_expr = self.parse_value_expr()
            comp_node = QueryTree("COMPARISON", op_token.value)
            comp_node.add_child(left_expr)
            comp_node.add_child(right_expr)
            return comp_node

        return left_expr

    def parse_in_array(self) -> QueryTree:
        self.expect(TokenType.DELIMITER_LPAREN)
        values = self.parse_value_list()
        self.expect(TokenType.DELIMITER_RPAREN)

        list_node = QueryTree("LIST", "")
        for v in values:
            list_node.add_child(v)
        return list_node

    def parse_in_list_or_subquery(self) -> QueryTree:
        if self.peek_token and self.peek_token.type == TokenType.KEYWORD_SELECT:
            self.expect(TokenType.DELIMITER_LPAREN)
            subquery = self.parse_subquery()
            self.expect(TokenType.DELIMITER_RPAREN)
            return subquery
        return self.parse_in_array()

    def parse_value_expr(self) -> QueryTree:
        return self.parse_add_sub()

    def parse_add_sub(self) -> QueryTree:
        node = self.parse_mul_div()
        while self.match(TokenType.OPERATOR_PLUS, TokenType.OPERATOR_MINUS):
            op = self.current_token.value
            self.advance()
            right = self.parse_mul_div()
            arith = QueryTree("ARITH_EXPR", op)
            arith.add_child(node)
            arith.add_child(right)
            node = arith
        return node

    def parse_mul_div(self) -> QueryTree:
        node = self.parse_unary()
        while self.match(TokenType.OPERATOR_MULTIPLY, TokenType.OPERATOR_DIVIDE):
            op = self.current_token.value
            self.advance()
            right = self.parse_unary()
            arith = QueryTree("ARITH_EXPR", op)
            arith.add_child(node)
            arith.add_child(right)
            node = arith
        return node

    def parse_unary(self) -> QueryTree:
        if self.match(TokenType.OPERATOR_PLUS, TokenType.OPERATOR_MINUS):
            op = self.current_token.value
            self.advance()
            operand = self.parse_unary()
            arith = QueryTree("ARITH_EXPR", op)
            arith.add_child(QueryTree("LITERAL_NUMBER", "0"))
            arith.add_child(operand)
            return arith
        return self.parse_primary_value()

    def parse_primary_value(self) -> QueryTree:
        if self.match(TokenType.DELIMITER_LPAREN):
            self.advance()
            expr = self.parse_value_expr()
            self.expect(TokenType.DELIMITER_RPAREN)
            return expr

        if self.match(TokenType.LITERAL_STRING, TokenType.LITERAL_NUMBER, TokenType.LITERAL_BOOLEAN, TokenType.LITERAL_NULL):
            literal_node = self.build_literal(self.current_token)
            self.advance()
            return literal_node

        if self.match(TokenType.IDENTIFIER):
            ident_value = self.current_token.value
            if self.peek_token and self.peek_token.type == TokenType.DELIMITER_LPAREN:
                self.advance()
                self.expect(TokenType.DELIMITER_LPAREN)
                args = []
                if not self.match(TokenType.DELIMITER_RPAREN):
                    args.append(self.parse_value_expr())
                    while self.match(TokenType.DELIMITER_COMMA):
                        self.advance()
                        args.append(self.parse_value_expr())
                self.expect(TokenType.DELIMITER_RPAREN)
                func_node = QueryTree("FUNCTION_CALL", ident_value)
                for arg in args:
                    func_node.add_child(arg)
                return func_node
            self.advance()
            return self.build_column_ref(ident_value)

        raise ParserError("Expected expression", self.current_token)

    def parse_subquery(self) -> QueryTree:
        return self.parse_select()

    def parse_identifier_list(self) -> List[str]:
        if not self.match(TokenType.IDENTIFIER):
            raise ParserError("Expected identifier", self.current_token)
        values = [self.current_token.value]
        self.advance()

        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            if not self.match(TokenType.IDENTIFIER):
                raise ParserError("Expected identifier after comma", self.current_token)
            values.append(self.current_token.value)
            self.advance()
        return values

    def parse_value_list(self) -> List[QueryTree]:
        values = [self.parse_value_expr()]
        while self.match(TokenType.DELIMITER_COMMA):
            self.advance()
            values.append(self.parse_value_expr())
        return values

    def build_column_ref(self, identifier: str) -> QueryTree:
        if "." in identifier:
            table, column = identifier.split(".", 1)
        else:
            table, column = None, identifier

        column_node = QueryTree("COLUMN_NAME", "")
        column_node.add_child(QueryTree("IDENTIFIER", column))

        col_ref = QueryTree("COLUMN_REF", "")
        col_ref.add_child(column_node)

        if table:
            table_node = QueryTree("TABLE_NAME", "")
            table_node.add_child(QueryTree("IDENTIFIER", table))
            col_ref.add_child(table_node)

        return col_ref

    def build_literal(self, token: Token) -> QueryTree:
        mapping = {
            TokenType.LITERAL_STRING: "LITERAL_STRING",
            TokenType.LITERAL_NUMBER: "LITERAL_NUMBER",
            TokenType.LITERAL_BOOLEAN: "LITERAL_BOOLEAN",
            TokenType.LITERAL_NULL: "LITERAL_NULL",
        }
        node_type = mapping.get(token.type, "LITERAL")
        return QueryTree(node_type, token.value)

    def _build_operator(self, operator: str, children: List[QueryTree]) -> QueryTree:
        node = QueryTree("OPERATOR", operator)
        for child in children:
            node.add_child(child)
        return node
    