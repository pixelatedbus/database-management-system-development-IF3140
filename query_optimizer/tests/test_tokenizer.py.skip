import pytest
from query_optimizer.tokenizer import Tokenizer
from query_optimizer.query_token import TokenType

"""
Cara ngetestnya mesti download pytest dulu
'pip install pytest'

habis tu pastiin ada di folder root terus tinggal ngetik command
'pytest' atau kalau output printnya pengen keliatan tinggal 'pytest -s'
"""

def test_query():

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
        print(f"Token diterima Parser: {token}")
        
        expected_type, expected_value = expected_tokens[idx]
        assert token.type == expected_type
        assert token.value == expected_value
        
        idx += 1
        if token.type == TokenType.EOF:
            break
            
    assert idx == len(expected_tokens)

def test_error_handling():
    query = "SELECT $col FROM users;"
    
    tokenizer = Tokenizer(query)
    
    tokenizer.get_next_token() 
    
    with pytest.raises(SyntaxError) as e:
        tokenizer.get_next_token()
    
    assert "Karakter tidak dikenal: '$'" in str(e.value)
    print("\nTes error berhasil ditangkap.")


# if __name__ == "__main__":
#     test_query()
#     test_error_handling()