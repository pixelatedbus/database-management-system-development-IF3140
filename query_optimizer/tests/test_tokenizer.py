import unittest
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.query_token import TokenType


class TestTokenizer(unittest.TestCase):

    def test_query(self):
        query = """
        SELECT id, name
        FROM users
        WHERE users.id = 10 AND name <> 'admin';
        """
        
        tokenizer = Tokenizer(query)
        
        expected_tokens = [
            (TokenType.KEYWORD_SELECT, "SELECT"),
            (TokenType.IDENTIFIER, "id"),
            (TokenType.DELIMITER_COMMA, ","),
            (TokenType.IDENTIFIER, "name"),
            (TokenType.KEYWORD_FROM, "FROM"),
            (TokenType.IDENTIFIER, "users"),
            (TokenType.KEYWORD_WHERE, "WHERE"),
            (TokenType.IDENTIFIER, "users.id"),
            (TokenType.OPERATOR_EQUAL, "="),
            (TokenType.LITERAL_NUMBER, "10"),
            (TokenType.KEYWORD_AND, "AND"),
            (TokenType.IDENTIFIER, "name"),
            (TokenType.OPERATOR_NOT_EQUAL, "<>"),
            (TokenType.LITERAL_STRING, "admin"),
            (TokenType.DELIMITER_SEMICOLON, ";"),
            (TokenType.EOF, "")
        ]
        
        idx = 0
        while True:
            token = tokenizer.get_next_token()

            
            expected_type, expected_value = expected_tokens[idx]
            
            self.assertEqual(token.type, expected_type, 
                             f"Token type mismatch at index {idx}. Got {token.type}, expected {expected_type}")
            self.assertEqual(token.value, expected_value, 
                             f"Token value mismatch at index {idx}. Got {token.value}, expected {expected_value}")
            
            idx += 1
            if token.type == TokenType.EOF:
                break
                
        self.assertEqual(idx, len(expected_tokens))

    def test_error_handling(self):
        query = "SELECT $col FROM users;"
        
        tokenizer = Tokenizer(query)
        
        # Skip SELECT token
        tokenizer.get_next_token()
        
        # Now the next token should be the invalid '$col'
        with self.assertRaises(SyntaxError) as cm:
            tokenizer.get_next_token()
        
        self.assertIn("Karakter tidak dikenal: '$'", str(cm.exception))

if __name__ == "__main__":
    unittest.main()