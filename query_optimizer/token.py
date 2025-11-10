import enum
from dataclasses import dataclass

class TokenType(enum.Enum):
    
    # Sementara gini dulu kali ya bentuk token typenya ntar kalau mau diubah bilang, kalau ngga juga bilang

    KEYWORD_BEGIN_TRANSACTION = "BEGIN TRANSACTION"
    KEYWORD_ORDER_BY = "ORDER BY"
    KEYWORD_SELECT = "SELECT"
    KEYWORD_FROM = "FROM"
    KEYWORD_WHERE = "WHERE"
    KEYWORD_JOIN = "JOIN"
    KEYWORD_ON = "ON"
    KEYWORD_NATURAL = "NATURAL"
    KEYWORD_UPDATE = "UPDATE"
    KEYWORD_SET = "SET"
    KEYWORD_INSERT = "INSERT"
    KEYWORD_INTO = "INTO"
    KEYWORD_DELETE = "DELETE"
    KEYWORD_LIMIT = "LIMIT"
    KEYWORD_COMMIT = "COMMIT"
    KEYWORD_AND = "AND"
    KEYWORD_OR = "OR"
    KEYWORD_NOT = "NOT"
    
    OPERATOR_NOT_EQUAL = "<>"
    OPERATOR_GREATER_EQUAL = ">="
    OPERATOR_LESS_EQUAL = "<="
    OPERATOR_EQUAL = "="
    OPERATOR_GREATER_THAN = ">"
    OPERATOR_LESS_THAN = "<"
    OPERATOR_PLUS = "+"
    OPERATOR_MINUS = "-"
    OPERATOR_MULTIPLY = "*"
    OPERATOR_DIVIDE = "/"
    
    IDENTIFIER = "IDENTIFIER"
    
    LITERAL_STRING = "LITERAL_STRING"
    LITERAL_NUMBER = "LITERAL_NUMBER"
    LITERAL_BOOLEAN = "LITERAL_BOOLEAN"
    LITERAL_NULL = "LITERAL_NULL"
    
    DELIMITER_COMMA = ","
    DELIMITER_SEMICOLON = ";"
    DELIMITER_LPAREN = "("
    DELIMITER_RPAREN = ")"
    
    UNKNOWN = "UNKNOWN"
    EOF = "EOF"

@dataclass
class Token:

    type: TokenType
    value: str
    line: int
    column: int 