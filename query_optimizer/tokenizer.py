import re
from .query_token import Token, TokenType

class Tokenizer:

    def __init__(self, query: str):
        self.query = query
        self.cursor = 0
        self.line = 1
        self.column = 1
        
        self.token_patterns = self._compile_patterns()

    def _compile_patterns(self):

        patterns = [
            (TokenType.KEYWORD_BEGIN_TRANSACTION, r'\bBEGIN TRANSACTION\b'),
            (TokenType.KEYWORD_ORDER_BY,  r'\bORDER BY\b'),
            (TokenType.KEYWORD_SELECT,    r'\bSELECT\b'),
            (TokenType.KEYWORD_FROM,      r'\bFROM\b'),
            (TokenType.KEYWORD_WHERE,     r'\bWHERE\b'),
            (TokenType.KEYWORD_EXIST,     r'\bEXIST\b'),
            (TokenType.KEYWORD_JOIN,      r'\bJOIN\b'),
            (TokenType.KEYWORD_INNER,     r'\bINNER\b'),
            (TokenType.KEYWORD_ON,        r'\bON\b'),
            (TokenType.KEYWORD_IN,        r'\bIN\b'),
            (TokenType.KEYWORD_NATURAL,   r'\bNATURAL\b'),
            (TokenType.KEYWORD_UPDATE,    r'\bUPDATE\b'),
            (TokenType.KEYWORD_SET,       r'\bSET\b'),
            (TokenType.KEYWORD_INSERT,    r'\bINSERT\b'),
            (TokenType.KEYWORD_INTO,      r'\bINTO\b'),
            (TokenType.KEYWORD_DELETE,    r'\bDELETE\b'),
            (TokenType.KEYWORD_CREATE,    r'\bCREATE\b'),
            (TokenType.KEYWORD_TABLE,     r'\bTABLE\b'),
            (TokenType.KEYWORD_DROP,      r'\bDROP\b'),
            (TokenType.KEYWORD_CASCADE,   r'\bCASCADE\b'),
            (TokenType.KEYWORD_RESTRICT,  r'\bRESTRICT\b'),
            (TokenType.KEYWORD_AS,        r'\bAS\b'),
            (TokenType.KEYWORD_EXISTS,    r'\bEXISTS\b'),
            (TokenType.KEYWORD_BETWEEN,   r'\bBETWEEN\b'),
            (TokenType.KEYWORD_IS,        r'\bIS\b'),
            (TokenType.KEYWORD_LIKE,      r'\bLIKE\b'),
            (TokenType.KEYWORD_FOREIGN,   r'\bFOREIGN\b'),
            (TokenType.KEYWORD_PRIMARY,   r'\bPRIMARY\b'),
            (TokenType.KEYWORD_KEY,       r'\bKEY\b'),
            (TokenType.KEYWORD_REFERENCES,r'\bREFERENCES\b'),
            (TokenType.KEYWORD_LIMIT,     r'\bLIMIT\b'),
            (TokenType.KEYWORD_COMMIT,    r'\bCOMMIT\b'),
            (TokenType.KEYWORD_ABORT,     r'\bABORT\b'),
            (TokenType.KEYWORD_AND,       r'\bAND\b'),
            (TokenType.KEYWORD_OR,        r'\bOR\b'),
            (TokenType.KEYWORD_NOT,       r'\bNOT\b'),
            
            (TokenType.LITERAL_BOOLEAN,   r'\b(TRUE|FALSE)\b'),
            (TokenType.LITERAL_NULL,      r'\bNULL\b'),
            (TokenType.LITERAL_NUMBER,    r'\b\d+(\.\d+)?\b'),
            (TokenType.LITERAL_STRING,    r"'[^']*'|\"[^\"]*\""),
            
            (TokenType.OPERATOR_NOT_EQUAL,   r'<>'),
            (TokenType.OPERATOR_GREATER_EQUAL, r'>='),
            (TokenType.OPERATOR_LESS_EQUAL,  r'<='),
            (TokenType.OPERATOR_EQUAL,       r'='),
            (TokenType.OPERATOR_GREATER_THAN,r'>'),
            (TokenType.OPERATOR_LESS_THAN,   r'<'),
            (TokenType.OPERATOR_PLUS,        r'\+'),
            (TokenType.OPERATOR_MINUS,       r'-'),
            (TokenType.OPERATOR_MULTIPLY,    r'\*'),
            (TokenType.OPERATOR_DIVIDE,      r'/'),

            (TokenType.IDENTIFIER,        r'\b[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)?\b'),

            (TokenType.DELIMITER_LPAREN,    r'\('),
            (TokenType.DELIMITER_RPAREN,    r'\)'),
            (TokenType.DELIMITER_COMMA,     r','),
            (TokenType.DELIMITER_SEMICOLON, r';'),
            
            (TokenType.UNKNOWN,           r'.'),
        ]

        combined_regex = "|".join(
            f'(?P<{token_type.name}>{pattern})' 
            for token_type, pattern in patterns
        )
        
        return re.compile(combined_regex, re.IGNORECASE)

    def _update_position(self, text: str):
        for char in text:
            if char == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1

    def _skip_whitespace(self):
        while self.cursor < len(self.query) and self.query[self.cursor].isspace():
            self._update_position(self.query[self.cursor])
            self.cursor += 1

    def get_next_token(self) -> Token:

        self._skip_whitespace()

        if self.cursor >= len(self.query):
            return Token(TokenType.EOF, "", self.line, self.column)

        match = self.token_patterns.match(self.query, self.cursor)

        if not match:
            raise SyntaxError("Kesalahan Tokenizer internal.")

        token_type_str = match.lastgroup
        token_type = TokenType[token_type_str]
        token_value = match.group(token_type_str)

        line_start = self.line
        column_start = self.column
        
        self.cursor = match.end()
        self._update_position(token_value)

        if token_type == TokenType.UNKNOWN:
            raise SyntaxError(
                f"Karakter tidak dikenal: '{token_value}' "
                f"di baris {line_start}, kolom {column_start}"
            )
            
        final_value = token_value
        if token_type.name.startswith("KEYWORD_") or token_type == TokenType.LITERAL_NULL:
            final_value = token_value.upper()
        elif token_type == TokenType.LITERAL_STRING:
            final_value = token_value[1:-1]
        
        return Token(
            type=token_type,
            value=final_value,
            line=line_start,
            column=column_start
        )
